import asyncio
from asyncio.log import logger
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import traceback
from typing import List, Optional, Dict, Union, Any, Tuple
import aiohttp
from aiohttp import ClientTimeout, BasicAuth, TCPConnector
from dataclasses import dataclass
from bs4 import BeautifulSoup, Tag
from functools import lru_cache
import json
from processors.web_content_processor import ContentProcessor
from creators.folder_creator import FolderCreator
from creators.document_creator import DocumentCreator
from creators.collapse_content_creator import CollapseContentProcessor
from cache.web_content_cache import ContentCache
from processors.web_content_mixed import MixedContentProcessor
from core.url_utils import UrlUtils

@dataclass
class ContentResponse:
    content_id: int
    is_new: bool
    error: Optional[str] = None

class WebContentCreator:
    # Precomputed constants
    DEFAULT_PORTLET_ID = 'com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_JournalCont_'
    ASSOCIATION_ENDPOINT = '/o/api-association-migrador/v1.0/journal-content/associate-article'
    
    def __init__(self, config):
        self.config = config
        self.session = None
        self.folder_creator = FolderCreator(config)
        self.document_creator = DocumentCreator(config)
        self.collapse_processor = CollapseContentProcessor(config)
        self.mixed_processor = MixedContentProcessor(config)
        self.url_utils = UrlUtils()
        self.cache = ContentCache()
        self.error_log_file = "content_migration_errors.txt"
        self.base_domain = ""
        self._connection_pool = None
        self._setup_logging()
        self._site_pages_url = f"{config.liferay_url}/o/headless-delivery/v1.0/sites/{config.site_id}/site-pages"
        self.content_processor = ContentProcessor(self)
        self._request_semaphore = asyncio.Semaphore(20)

    def _setup_logging(self):
        """Configura o logging"""
        if logger.handlers:
            return

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handlers = [
            (logging.StreamHandler(), logging.INFO),
            (RotatingFileHandler('migration.log', maxBytes=10*1024*1024, backupCount=5), logging.INFO),
            (RotatingFileHandler(self.error_log_file, maxBytes=5*1024*1024, backupCount=3), logging.ERROR)
        ]

        for handler, level in handlers:
            handler.setFormatter(formatter)
            handler.setLevel(level)
            logger.addHandler(handler)
        
        logger.setLevel(logging.INFO)

    async def initialize_session(self):
        """Inicializa sessão HTTP com limites ampliados e pool de conexões"""
        if self.session:
            await self.session.close()
        
        # Cria pool de conexões se não existir
        if not self._connection_pool:
            self._connection_pool = TCPConnector(
                ssl=False,
                limit=100,  # Aumentado para 100 conexões simultâneas
                ttl_dns_cache=1200,  # Cache de DNS por 20 minutos
                keepalive_timeout=120,  # Mantém conexões por mais tempo
                force_close=False  # Permite reuso de conexões
            )
        
        timeout = ClientTimeout(total=180, connect=30, sock_read=60, sock_connect=30)
        auth = BasicAuth(login=self.config.liferay_user, password=self.config.liferay_pass)
        
        self.session = aiohttp.ClientSession(
            auth=auth,
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'User-Agent': 'ContentMigrator/2.0'
            },
            timeout=timeout,
            connector=self._connection_pool,
            json_serialize=json.dumps  # Serialização JSON mais rápida
        )
        
        logger.info("Inicializando sessões dos criadores em paralelo")
        await asyncio.gather(
            self.folder_creator.initialize_session(),
            self.document_creator.initialize_session()
        )
        logger.info("Sessões inicializadas com sucesso")

    def _log_error(self, error_type: str, url: str, error_msg: str, title: str = "", hierarchy: List[str] = None):
        """Log centralizado de erros"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        hierarchy_str = ' > '.join(hierarchy) if hierarchy else ''
        
        error_entry = (
            f"\n=== {error_type} Error Log Entry: {timestamp} ===\n"
            f"URL: {url}\n"
            f"Title: {title}\n"
            f"Hierarchy: {hierarchy_str}\n"
            f"Error: {error_msg}\n"
            f"{'=' * 50}\n"
        )
        
        try:
            with open(self.error_log_file, "a", encoding="utf-8") as f:
                f.write(error_entry)
        except Exception as e:
            logger.error(f"Error writing to error log: {str(e)}")

    async def _retry_operation(self, operation, *args, max_retries=3, **kwargs):
        """Wrapper para operações com retry exponencial otimizado"""
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return await operation(*args, **kwargs)
            except aiohttp.ClientResponseError as e:
                # Falhas específicas para erros HTTP
                last_error = e
                if e.status in (429, 503, 504):  # Rate limiting ou serviço indisponível
                    wait_time = 2 * (attempt + 1)
                    logger.warning(f"Rate limit ou serviço indisponível ({e.status}). Tentativa {attempt+1}/{max_retries}. Aguardando {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                elif e.status in (400, 401, 403, 404, 405):  # Erros de cliente - não adianta tentar novamente
                    logger.error(f"Erro de cliente ({e.status}): {str(e)}")
                    raise e
                # Outros erros HTTP - tentar novamente
                if attempt < max_retries - 1:
                    wait_time = 1 * (attempt + 1)
                    logger.warning(f"Erro HTTP ({e.status}). Tentativa {attempt+1}/{max_retries}. Aguardando {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                raise last_error
            except Exception as e:
                # Falhas gerais
                last_error = e
                if attempt < max_retries - 1:
                    wait_time = 1 * (attempt + 1)
                    logger.warning(f"Erro geral: {str(e)}. Tentativa {attempt+1}/{max_retries}. Aguardando {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                raise last_error

    async def _controlled_request(self, method: str, url: str, **kwargs) -> Tuple[int, Any]:
        """Executa requisição HTTP com controle de concorrência e melhor tratamento de erros"""
        async with self._request_semaphore:
            if not self.session:
                await self.initialize_session()
                
            try:
                async with getattr(self.session, method)(url, **kwargs) as response:
                    status = response.status
                    
                    try:
                        if response.content_type == 'application/json':
                            data = await response.json(content_type=None)
                        else:
                            data = await response.text()
                            # Try to parse as JSON even if content-type isn't JSON
                            try:
                                data = json.loads(data)
                            except json.JSONDecodeError:
                                pass
                    except Exception as e:
                        logger.warning(f"Error parsing response: {str(e)}")
                        data = await response.text()
                    
                    return status, data
                    
            except aiohttp.ClientResponseError as e:
                logger.error(f"HTTP error during {method} to {url}: {e.status} - {str(e)}")
                raise
            except aiohttp.ClientError as e:
                logger.error(f"Client error during {method} to {url}: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error during {method} to {url}: {str(e)}")
                raise
            
    @lru_cache(maxsize=200)
    def _get_content_url(self, folder_id: int) -> str:
        """Retorna URL de criação de conteúdo (cached)"""
        return f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{folder_id}/structured-contents"

    async def fetch_content(self, url: str) -> str:
        """Busca conteúdo da URL otimizada"""
        cached_content = self.cache.get_url(url)
        if cached_content:
            return cached_content

        if not self.session:
            await self.initialize_session()

        try:
            if not self.base_domain:
                self.base_domain = self.url_utils.extract_domain(url)

            full_url = self.url_utils.build_url(url, self.base_domain)
            if not full_url:
                raise ValueError(f"Invalid URL: {url}")

            status, html = await self._controlled_request('get', full_url)
            if status != 200:
                raise Exception(f"Failed to fetch content: {status}")
            
            # Parse HTML apenas uma vez
            soup = BeautifulSoup(html, 'html.parser')
            
            for selector in self.content_processor.CONTENT_SELECTORS:
                content_tag_type = 'id' if selector['type'] == 'id' else 'class_'
                content = soup.find(**{content_tag_type: selector['value']})
                
                if content:
                    cleaned_content = self.content_processor._clean_content(str(content))
                    self.cache.add_url(url, cleaned_content)
                    return cleaned_content

            raise Exception("No valid content found")

        except Exception as e:
            self._log_error("Content Fetch", url, str(e))
            return ''
    
    async def create_structured_content(self, title: str, html_content: str, folder_id: int) -> Dict[str, Union[int, str]]:
        """Cria conteúdo estruturado e retorna tanto o ID quanto a key"""
        if not self.session:
            await self.initialize_session()
        
        friendly_url = self.url_utils.sanitize_content_path(title)

        try:
            content_data = {
                "contentStructureId": self.config.content_structure_id,
                "contentFields": [
                    {
                        "contentFieldValue": {
                            "data": html_content
                        },
                        "name": "content"
                    }
                ],
                "structuredContentFolderId": folder_id,
                "title": title,
                "friendlyUrlPath": friendly_url
            }

            url = self._get_content_url(folder_id)

            async def create_attempt():
                status, result = await self._controlled_request('post', url, json=content_data)
                if status in (200, 201):
                    content_id = result.get('id')
                    content_key = result.get('key')
                    if content_id:
                        logger.info(f"Created content: {title} (ID: {content_id}, Key: {content_key})")
                        self.cache.add_content(title, content_id)
                        return {"id": int(content_id), "key": content_key}
                
                raise Exception(f"Failed to create content: {status} - {result}")

            return await self._retry_operation(create_attempt)
            
        except Exception as e:
            self._log_error("Content Creation", title, str(e))
            return {"id": 0, "key": ""}
    
    async def migrate_content(self, source_url: str, title: str, hierarchy: List[str]) -> Dict[str, Union[int, str]]:
        """Migra conteúdo e retorna tanto o ID quanto a key do conteúdo"""
        try:
            # Verifica cache primeiro para evitar operações desnecessárias
            cached_content_id = self.cache.get_content(title)
            if cached_content_id:
                logger.info(f"Using cached content for {title}")
                # Buscar a key se estiver usando cache
                url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{cached_content_id}"
                status, data = await self._controlled_request('get', url)
                if status == 200:
                    return {"id": cached_content_id, "key": data.get('key')}
                return {"id": cached_content_id, "key": str(cached_content_id)}

            # Primeiro busca os dados da página
            page_data = await self.find_page_by_title_or_id(title)
            logger.info(f"Found page data: {bool(page_data)}")

            # Cria recursos necessários em paralelo para melhor performance
            folder_results = await asyncio.gather(
                self.folder_creator.create_folder_hierarchy(hierarchy, title, 'journal'),
                self.folder_creator.create_folder_hierarchy(hierarchy, title, 'documents')
            )
            folder_id, folder_id_dl = folder_results

            if not folder_id:
                raise Exception(f"Could not create/find folder for: {title}")

            # Verifica conteúdo existente - evita duplicação
            existing_content = await self.find_existing_content(title, folder_id)
            if existing_content:
                if page_data:
                    await self.associate_content_with_page_portlet(existing_content, page_data)
                return existing_content

            # Processa o conteúdo - com lógica otimizada
            process_result = await self.content_processor.fetch_and_process_content(source_url, folder_id_dl)
            if not process_result["success"]:
                raise Exception(process_result["error"])

            content_result = None
            content_results = []

            # Processamento condicional baseado no tipo de conteúdo
            if process_result["is_collapsible"]:
                logger.info(f"Creating collapsible content for {title}")
                content_result = await self.collapse_processor.create_collapse_content(
                    self, title, process_result["content"], folder_id
                )
            elif process_result["has_mixed_content"]:
                logger.info(f"Creating mixed content for {title}")
                content_results = await self.mixed_processor.process_mixed_content(
                    self, title, process_result["content"], folder_id, folder_id_dl, source_url
                )
                content_result = content_results[0] if content_results else None
            else:
                logger.info(f"Creating regular content for {title}")
                content_result = await self.create_structured_content(
                    title, process_result["content"], folder_id
                )

            if not content_result:
                raise Exception("Failed to create content in Liferay")

            # Associa conteúdos à página de forma otimizada
            if page_data:
                association_tasks = []
                if content_results:
                    for i, result in enumerate(content_results):
                        association_tasks.append(self.associate_content_with_page_portlet(result, page_data, portlet_index=i))
                else:
                    association_tasks.append(self.associate_content_with_page_portlet(content_result, page_data))
                
                # Executa todas as associações em paralelo
                if association_tasks:
                    await asyncio.gather(*association_tasks)

            self.cache.add_content(title, content_result)
            return content_result

        except Exception as e:
            error_msg = f"Error during content migration: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self._log_error("Content Migration", source_url, error_msg, title, hierarchy)
            return {"id": 0, "key": ""}
    
    async def find_existing_content(self, title: str, folder_id: int) -> Optional[Dict[str, Union[int, str]]]:
        """Busca conteúdo existente com cache aprimorado"""
        # Verifica cache primeiro
        cached_id = self.cache.get_content(title)
        if cached_id:
            logger.info(f"Found cached content for {title}: {cached_id}")
            return {"id": cached_id, "key": str(cached_id)}

        if not self.session:
            await self.initialize_session()

        try:
            url = self._get_content_url(folder_id)
            
            # Escape special characters in title for filter
            safe_title = title.replace("'", "\\'")
            
            params = {
                'filter': f"title eq '{safe_title}'",
                'fields': 'id,title,key',
                'page': 1,
                'pageSize': 5  # Busca mais resultados para comparação exata
            }
            
            status, data = await self._controlled_request('get', url, params=params)
            if status == 200:
                for content in data.get('items', []):
                    # Comparação case-insensitive para maior compatibilidade
                    if content['title'].lower() == title.lower():
                        content_id = int(content['id'])
                        content_key = content['key']
                        result = {"id": content_id, "key": content_key}
                        self.cache.add_content(title, content_id)
                        logger.info(f"Found existing content: {title} (ID: {content_id}, Key: {content_key})")
                        return result
                            
            logger.info(f"No existing content found with title: {title}")
            return None
                    
        except Exception as e:
            self._log_error("Content Search", title, str(e))
            return None

    @lru_cache(maxsize=100)
    def _parse_content_portlets(self, html_content: str) -> List[Dict[str, str]]:
        """Parse portlets de conteúdo de HTML com caching"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            portlets = []
            journal_portlets = soup.find_all(
                lambda tag: isinstance(tag, Tag) and tag.get('id', '').startswith('p_p_id_com_liferay_journal_content_web_portlet_JournalContentPortlet')
            )
            
            for portlet in journal_portlets:
                portlet_id = portlet.get('id', '').replace('p_p_id_', '')
                if portlet_id:
                    portlets.append({
                        'portletId': portlet_id,
                        'articleId': ''
                    })
            
            # Se não encontrou nenhum portlet, cria um padrão
            if not portlets:
                portlets.append({
                    'portletId': self.DEFAULT_PORTLET_ID,
                    'articleId': ''
                })

            return portlets
        except Exception as e:
            logger.error(f"Error parsing portlets: {str(e)}")
            return [{
                'portletId': self.DEFAULT_PORTLET_ID,
                'articleId': ''
            }]

    async def find_page_by_title_or_id(self, identifier: Union[str, int]) -> Optional[Dict]:
        """
        Busca uma página pelo título ou ID com pesquisa otimizada
        """
        try:
            if not self.session:
                await self.initialize_session()

            # Otimizado para buscar páginas de forma mais eficiente
            search_term = str(identifier).lower()
            is_numeric = search_term.isdigit()
            
            # Determina estratégia de busca baseada no tipo de identificador
            params = {
                'page': 1,
                'pageSize': 100,
                'fields': 'id,title,friendlyUrlPath'
            }
            
            if is_numeric:
                # Se for um ID, adiciona filtro específico
                params['filter'] = f"id eq {search_term}"
            else:
                # Se for título, usa busca de texto
                params['search'] = search_term
            
            status, data = await self._controlled_request('get', self._site_pages_url, params=params)
            if status != 200:
                logger.error(f"Failed to search pages. Status: {status}")
                return None
                
            items = data.get('items', [])
            if not items:
                return None
                
            # Procura correspondência exata primeiro
            page_data = None
            for item in items:
                item_title = item.get('title', '').lower()
                if (is_numeric and str(item.get('id')) == search_term) or item_title == search_term:
                    page_data = item
                    break
            
            # Se não encontrou exata, tenta parcial
            if not page_data and not is_numeric:
                for item in items:
                    if search_term in item.get('title', '').lower():
                        page_data = item
                        break
            
            if not page_data:
                return None

            # Busca rendered page para obter portlets
            friendly_url = page_data.get('friendlyUrlPath', '').strip('/')
            if not friendly_url:
                # Fallback para id se não tiver friendly URL
                page_id = page_data.get('id')
                if not page_id:
                    return None
                friendly_url = str(page_id)

            rendered_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/site-pages/{friendly_url}/rendered-page"
            
            headers = {
                'Accept': 'text/html'
            }
            
            status, rendered_html = await self._controlled_request('get', rendered_url, headers=headers)
            if status == 200:
                # Parse portlets
                portlets = self._parse_content_portlets(rendered_html)
                page_data['portlets'] = portlets
                return page_data
                
            return None
                
        except Exception as e:
            logger.error(f"Error finding page: {str(e)}")
            return None

    async def get_journal_portlet_instance(self, page_data: Dict) -> Optional[str]:
        """
        Obtém o ID do portlet Journal Content disponível na página
        """
        try:
            portlets = page_data.get('portlets', [])
            
            # Primeiro tenta encontrar um portlet sem conteúdo associado
            for portlet in portlets:
                portlet_id = portlet.get('portletId', '')
                if portlet_id and not portlet.get('articleId'):
                    return portlet_id
            
            # Se não encontrou vazio, usa o primeiro disponível
            if portlets:
                portlet_id = portlets[0].get('portletId')
                if portlet_id:
                    return portlet_id
                    
            # Se não encontrou nenhum, usa o padrão
            return self.DEFAULT_PORTLET_ID
            
        except Exception as e:
            logger.error(f"Error getting portlet instance: {str(e)}")
            return self.DEFAULT_PORTLET_ID

    async def associate_content_with_page_portlet(self, content: Union[Dict[str, Union[int, str]], str, int], 
                                                page_data: Union[Dict, int, str], 
                                                portlet_index: int = 0) -> bool:
        """
        Associa um conteúdo ao portlet Journal Content de uma página com retry aprimorado
        
        Args:
            content: Conteúdo a ser associado
            page_data: Dados da página
            portlet_index: Índice do portlet a ser usado (0 = primeiro, 1 = segundo, etc.)
        """
        try:
            # Busca página se necessário
            if not isinstance(page_data, dict):
                page_info = await self.find_page_by_title_or_id(page_data)
                if not page_info:
                    logger.warning(f"Page not found for identifier: {page_data}")
                    return False
            else:
                page_info = page_data
            
            # Obter todos os portlets disponíveis
            portlets = page_info.get('portlets', [])
            
            # Verificar se há portlets suficientes
            if not portlets:
                logger.warning(f"No portlets found for page: {page_info.get('title')}")
                return False
            
            # Selecionar o portlet pelo índice especificado
            if portlet_index >= len(portlets):
                logger.warning(f"Portlet index {portlet_index} out of range, using first available portlet")
                portlet_id = portlets[0].get('portletId', self.DEFAULT_PORTLET_ID)
            else:
                portlet_id = portlets[portlet_index].get('portletId', self.DEFAULT_PORTLET_ID)
            
            # Normaliza ID do portlet
            if not portlet_id.startswith('p_p_id_'):
                portlet_id = portlet_id.replace('p_p_id_', '')

            if portlet_id.endswith('_'):
                portlet_id = portlet_id[:-1]
            
            # Normaliza key do conteúdo
            content_key = content.get('key') if isinstance(content, dict) else str(content)
            
            # URL de associação
            association_url = f"{self.config.liferay_url}{self.ASSOCIATION_ENDPOINT}"

            params = {
                'plid': str(page_info.get('id')),
                'portletId': portlet_id,
                'articleId': content_key
            }

            async def associate_content():
                status, result = await self._controlled_request('post', association_url, params=params)
                if status in (200, 201):
                    logger.info(f"Association result (portlet index {portlet_index}): {result}")
                    return result.get('status') == 'SUCCESS'
                raise Exception(f"Association request failed with status {status}")

            # Tenta associar com retry
            return await self._retry_operation(associate_content, max_retries=4)
            
        except Exception as e:
            logger.error(f"Error associating content to portlet {portlet_index}: {str(e)}")
            return False
        
    async def create_and_associate_content(self, source_url: str, title: str, hierarchy: List[str], 
                                        page_identifier: Union[str, int], page_friendly_url: str = None) -> ContentResponse:
        """
        Cria conteúdo e associa a um portlet específico da página
        """
        try:
            # Primeiro cria o conteúdo
            content_result = await self.migrate_content(source_url, title, hierarchy)
            if not content_result or not content_result.get('id'):
                raise Exception("Failed to create content")

            # Tenta associar ao portlet da página
            association_success = await self.associate_content_with_page_portlet(
                content_result, page_identifier
            )

            if not association_success:
                logger.warning(f"Content created but association failed: {title}")
                return ContentResponse(
                    content_id=content_result['id'],
                    is_new=True,
                    error="Content created but page association failed"
                )

            return ContentResponse(content_id=content_result['id'], is_new=True)

        except Exception as e:
            error_msg = f"Error in content creation and association: {str(e)}"
            logger.error(error_msg)
            self._log_error("Content Association", source_url, error_msg, title, hierarchy)
            return ContentResponse(content_id=0, is_new=False, error=error_msg)
    
    async def close(self):
        """Fecha recursos de forma eficiente"""
        if not self.session:
            return
            
        try:
            close_tasks = [
                self.session.close(),
                self.folder_creator.close(),
                self.document_creator.close()
            ]
            
            await asyncio.gather(*close_tasks)
            
            # Limpa pool de conexões
            if self._connection_pool:
                await self._connection_pool.close()
                self._connection_pool = None
            
            self.session = None
            self.cache.clear_all()
            logger.info("Recursos fechados com sucesso")
        except Exception as e:
            logger.error(f"Error closing resources: {str(e)}")