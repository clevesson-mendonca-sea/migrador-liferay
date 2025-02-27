import asyncio
from asyncio.log import logger
from datetime import datetime, time
import logging
from logging.handlers import RotatingFileHandler
import traceback
from typing import List, Optional, Dict, Union, Any, Tuple
import aiohttp
from aiohttp import ClientTimeout, BasicAuth, TCPConnector
from dataclasses import dataclass
from bs4 import BeautifulSoup, Comment, Tag
from functools import lru_cache
import json
from processors.web_content_processor import ContentProcessor
from creators.folder_creator import FolderCreator
from creators.document_creator import DocumentCreator
from creators.collapse_content_creator import CollapseContentProcessor
from creators.tab_content_creator import TabContentProcessor
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
        self.tab_processor = TabContentProcessor(config)
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
        self._background_tasks = set()
        self._associated_content_ids = set()
        self._association_lock = asyncio.Lock()
        
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
          
    async def _check_cached_content(self, title: str, html_content: Optional[str] = None) -> Optional[Dict[str, Union[int, str]]]:
        """
        Verifica o conteúdo em cache com validação adicional
        
        Args:
            title: Título do conteúdo
            html_content: Conteúdo HTML para comparação detalhada (opcional)
        
        Returns:
            Dicionário com detalhes do conteúdo em cache ou None
        """
        try:
            # Busca conteúdos em cache para o título
            cached_content_ids = self.cache.get_contents(title)
            
            if not cached_content_ids:
                return None
            
            # Verifica cada conteúdo em cache
            for cached_content_id in cached_content_ids:
                try:
                    # Busca detalhes do conteúdo
                    url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{cached_content_id}"
                    status, data = await self._controlled_request('get', url)
                    
                    if status == 200:
                        # Se não há conteúdo para comparação, retorna o primeiro
                        if not html_content:
                            return {"id": cached_content_id, "key": data.get('key')}
                        
                        # Compara o conteúdo se fornecido
                        content_fields = data.get('contentFields', [])
                        content_field = next((field for field in content_fields 
                                            if field.get('name') == 'content'), None)
                        
                        if content_field:
                            existing_content = content_field.get('contentFieldValue', {}).get('data', '')
                            
                            # Usa BeautifulSoup para comparação de conteúdo limpo
                            from bs4 import BeautifulSoup
                            import re
                            
                            def clean_content(content):
                                if not content:
                                    return ''
                                soup = BeautifulSoup(content, 'html.parser')
                                # Remove comentários, normaliza espaços
                                for comment in soup.find_all(text=lambda text: isinstance(text, Comment)):
                                    comment.extract()
                                # Remove tags de estilo, remove espaços extras
                                text = re.sub(r'\s+', ' ', soup.get_text()).strip()
                                return text
                            
                            # Compara conteúdos limpos
                            if clean_content(existing_content) == clean_content(html_content):
                                logger.info(f"Using cached content with exact match: {title} (ID: {cached_content_id})")
                                return {"id": cached_content_id, "key": data.get('key')}
                            
                            # Se os conteúdos forem diferentes, continua para o próximo
                            logger.debug(f"Cached content {cached_content_id} does not match current content")
                    
                except Exception as e:
                    logger.warning(f"Error checking cached content {cached_content_id}: {str(e)}")
            
            # Nenhum conteúdo em cache corresponde
            return None
            
        except Exception as e:
            logger.error(f"Error in cache check for {title}: {str(e)}")
            return None
            
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

        # Limitar título se necessário (por exemplo, 255 caracteres é um limite comum)
        if len(title) > 255:
            truncated_title = title[:250] + "..."
            logger.info(f"Título truncado para criação de conteúdo: '{title}' -> '{truncated_title}'")
            title = truncated_title

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
        try:
            # Executa operações de busca de página e criação de pastas em paralelo
            page_data, folder_results = await asyncio.gather(
                self.find_page_by_title_or_id(title),
                asyncio.gather(
                    self.folder_creator.create_folder_hierarchy(hierarchy, title, 'journal'),
                    self.folder_creator.create_folder_hierarchy(hierarchy, title, 'documents')
                )
            )
            
            # Desempacota resultados de pastas
            folder_id, folder_id_dl = folder_results

            if not folder_id:
                raise Exception(f"Could not create/find folder for: {title}")

            # Verifica conteúdo existente - evita duplicação
            existing_content = await self.find_existing_content(title, folder_id)
            if existing_content:
                # Associa em background se página existir
                if page_data:
                    await self._background_associate_content(existing_content, page_data)
                return existing_content

            # Usa fetch_and_process_content com skip_images=True
            processed_content = await self.content_processor.fetch_and_process_content(
                source_url, 
                folder_id=folder_id_dl, 
                skip_images=True
            )

            # Verifica se o processamento foi bem-sucedido
            if not processed_content.get('success'):
                raise Exception(processed_content.get('error', 'Failed to process content'))

            # Extrai o conteúdo processado
            cleaned_content = processed_content['content']
            
            # Verifica cache primeiro para evitar operações desnecessárias
            cached_content = await self._check_cached_content(title, cleaned_content)
            if cached_content:
                return cached_content
            
            # Determina tipo de conteúdo de forma mais eficiente
            content_type = processed_content.get('collapsible_type', 'none')
            logger.info(f"Processing content: {title} (type: {content_type})")

            # Processamento condicional com chamadas assíncronas mais eficientes
            content_result = None
            content_results = []

            # Usa dicionário de mapeamento para simplificar lógica
            content_type_handlers = {
                'tabs': self.tab_processor.create_tab_content,
                'panel': self.collapse_processor.create_collapse_content,
                'button': self.collapse_processor.create_collapse_content,
                'mixed': self.mixed_processor.process_mixed_content
            }

            # Seleciona o processador apropriado
            handler = content_type_handlers.get(content_type)
            if handler:
                if content_type == 'mixed':
                    content_results = await handler(
                        self, title, cleaned_content, folder_id, folder_id_dl, source_url
                    )
                    content_result = content_results[0] if content_results else None
                else:
                    content_result = await handler(
                        self, title, cleaned_content, folder_id, source_url, folder_id_dl
                    )
            else:
                # Fallback para criação de conteúdo regular
                content_result = await self.create_structured_content(
                    title, cleaned_content, folder_id
                )

            if not content_result:
                raise Exception("Failed to create content in Liferay")

            # Prepara resultados para associação
            content_results_for_association = content_results if content_results else [content_result]

            # Associa conteúdos à página de forma otimizada
            if page_data:
                # Usa list comprehension para criar tarefas de associação
                association_tasks = [
                    self._background_associate_content(result, page_data, portlet_index=i)
                    for i, result in enumerate(content_results_for_association)
                ]
                
                # Executa associações em paralelo
                await asyncio.gather(*association_tasks)

            # Agenda processamento de imagens para conteúdo regular
            if content_type not in ('tabs', 'panel', 'button', 'mixed'):
                content_id = content_result.get("id", 0)
                if content_id:
                    self._schedule_background_update(
                        content_id, 
                        title, 
                        cleaned_content, 
                        source_url, 
                        folder_id_dl
                    )
            
            # Adiciona ao cache
            # self.cache.add_content(title, content_result)

            return content_result

        except Exception as e:
            error_msg = f"Error during content migration: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self._log_error("Content Migration", source_url, error_msg, title, hierarchy)
            return {"id": 0, "key": ""}
    
    async def find_existing_content(self, title: str, folder_id: int, html_content: Optional[str] = None) -> Optional[Dict[str, Union[int, str]]]:
        """
        Busca conteúdo existente com verificação adicional de conteúdo
        
        Args:
            title: Título do conteúdo
            folder_id: ID da pasta
            html_content: Conteúdo HTML para comparação (opcional)
        
        Returns:
            Dicionário com detalhes do conteúdo existente ou None
        """
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
                'fields': 'id,title,key,contentFields',
                'page': 1,
                'pageSize': 20  # Aumenta o número de resultados para busca detalhada
            }
            
            status, data = await self._controlled_request('get', url, params=params)
            if status == 200:
                # Filtrar conteúdos com o mesmo título
                matching_contents = []
                for content in data.get('items', []):
                    # Comparação case-insensitive para título
                    if content['title'].lower() == title.lower():
                        # Se conteúdo HTML fornecido, compara detalhadamente
                        if html_content:
                            # Busca o campo de conteúdo
                            content_field = next((field for field in content.get('contentFields', []) 
                                                if field.get('name') == 'content'), None)
                            
                            if content_field:
                                existing_content = content_field.get('contentFieldValue', {}).get('data', '')
                                
                                # Usa BeautifulSoup para comparação de conteúdo limpo
                                from bs4 import BeautifulSoup
                                
                                # Remove tags, espaços extras, etc. para comparação
                                def clean_content(content):
                                    if not content:
                                        return ''
                                    soup = BeautifulSoup(content, 'html.parser')
                                    # Remove comentários, normaliza espaços
                                    for comment in soup.find_all(text=lambda text: isinstance(text, Comment)):
                                        comment.extract()
                                    return ' '.join(soup.get_text().split())
                                
                                # Compara conteúdos limpos
                                if clean_content(existing_content) == clean_content(html_content):
                                    content_id = int(content['id'])
                                    content_key = content['key']
                                    matching_contents.append({
                                        "id": content_id, 
                                        "key": content_key,
                                        "match_score": 1  # Conteúdo idêntico
                                    })
                                else:
                                    # Pode ser quase igual, adiciona com pontuação menor
                                    content_id = int(content['id'])
                                    content_key = content['key']
                                    matching_contents.append({
                                        "id": content_id, 
                                        "key": content_key,
                                        "match_score": 0.5  # Conteúdo similar
                                    })
                        else:
                            # Se não tiver conteúdo para comparar, retorna o primeiro
                            content_id = int(content['id'])
                            content_key = content['key']
                            matching_contents.append({
                                "id": content_id, 
                                "key": content_key,
                                "match_score": 0.5
                            })
                
                # Ordena por match_score decrescente
                if matching_contents:
                    best_match = max(matching_contents, key=lambda x: x['match_score'])
                    logger.info(f"Found existing content: {title} (ID: {best_match['id']}, Key: {best_match['key']})")
                    
                    # Adiciona ao cache
                    self.cache.add_content(title, best_match['id'])
                    
                    return {
                        "id": best_match['id'], 
                        "key": best_match['key']
                    }
                
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
                    logger.info(f"[ASSOCIAÇÃO] ✅ {result.get('message')}")
                    return result.get('status') == 'SUCCESS'
                raise Exception(f"[ASSOCIAÇÃO] ❌ {status}, {result}")

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

    async def update_content_with_processed_images(self, content_id: int, html_content: str, source_url: str, folder_id_dl: int) -> bool:
        """
        Atualiza um conteúdo já criado com imagens processadas
        
        Args:
            content_id: ID do conteúdo a atualizar
            html_content: Conteúdo HTML sem imagens processadas
            source_url: URL de origem para referência
            folder_id_dl: ID da pasta de documentos
            
        Returns:
            bool: True se a atualização foi bem-sucedida
        """
        try:
            # Verifica se o ContentUpdater já foi inicializado
            if not hasattr(self, 'content_updater'):
                from updaters.content_update import ContentUpdater
                self.content_updater = ContentUpdater(self.config)
                await self.content_updater.initialize_session()
                
            logger.info(f"Processando imagens para conteúdo ID {content_id}")
            
            # Processa as imagens usando o ContentUpdater existente
            processed_html = await self.content_updater.process_content_images(
                content=html_content,
                folder_id=folder_id_dl,
                base_url=source_url
            )
            
            # Verifica se houve mudanças
            if processed_html == html_content:
                logger.info(f"Nenhuma alteração de imagem necessária para o conteúdo ID {content_id}")
                return True
                
            # Preparar payload para atualização via Headless API
            update_data = {
                "contentFields": [
                    {
                        "contentFieldValue": {
                            "data": processed_html
                        },
                        "name": "content"
                    }
                ]
            }
            
            # Atualizar o conteúdo
            url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{content_id}"
            
            status, result = await self._controlled_request('patch', url, json=update_data)
            
            
            if status in (200, 201, 204):
                logger.info(f"✅ Conteúdo {content_id} atualizado com imagens processadas")
                return True
                
            logger.error(f"Erro ao atualizar conteúdo {content_id}: {status} - {result}")
            return False
            
        except Exception as e:
            error_msg = f"Erro ao atualizar conteúdo com imagens: {str(e)}"
            logger.error(error_msg)
            self._log_error("Content Update", str(content_id), error_msg)
            return False
    
    async def _background_update_content_with_images(self, content_id: int, title: str, html_content: str, source_url: str, folder_id_dl: int):
        """
        Função em background para processar imagens e atualizar conteúdo
        sem bloquear o fluxo principal de migração
        """
        try:
            logger.info(f"[BACKGROUND] Iniciando processamento de imagens para {title} (ID: {content_id})")
            
            # Verifica se o ContentUpdater já foi inicializado
            if not hasattr(self, 'content_updater'):
                from updaters.content_update import ContentUpdater
                self.content_updater = ContentUpdater(self.config)
                await self.content_updater.initialize_session()
            
            # Processa as imagens usando o ContentUpdater existente
            processed_html = await self.content_updater.process_content_images(
                content=html_content,
                folder_id=folder_id_dl,
                base_url=source_url
            )
            
            # Verifica se houve mudanças
            if processed_html == html_content:
                logger.info(f"[BACKGROUND] Nenhuma alteração de imagem necessária para {title}")
                return
            
            # Preparar payload para atualização via Headless API
            update_data = {
                "contentFields": [
                    {
                        "contentFieldValue": {
                            "data": processed_html
                        },
                        "name": "content"
                    }
                ]
            }
            
            # Atualizar o conteúdo
            url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{content_id}"
            
            status, result = await self._controlled_request('patch', url, json=update_data)
            
            if status in (200, 201, 204):
                logger.info(f"[BACKGROUND] ✅ Conteúdo {title} (ID: {content_id}) atualizado com imagens processadas")
            else:
                logger.error(f"[BACKGROUND] Erro ao atualizar conteúdo {title} (ID: {content_id}): {status}")
        
        except Exception as e:
            error_msg = f"[BACKGROUND] Erro ao atualizar conteúdo {title} com imagens: {str(e)}"
            logger.error(error_msg)
            self._log_error("Background Content Update", source_url, error_msg, title)
        finally:
            # Remove a tarefa da lista de tarefas em background
            if task := asyncio.current_task():
                self._background_tasks.discard(task)

    def _schedule_background_update(self, content_id: int, title: str, html_content: str, source_url: str, folder_id_dl: int):
        """
        Agenda a atualização de imagens para ser executada em background
        
        Args:
            content_id: ID do conteúdo a ser atualizado
            title: Título do conteúdo (para logs)
            html_content: Conteúdo HTML sem processamento de imagens
            source_url: URL de origem
            folder_id_dl: ID da pasta de documentos
        """
        # Cria uma tarefa em background
        task = asyncio.create_task(
            self._background_update_content_with_images(
                content_id, title, html_content, source_url, folder_id_dl
            )
        )
        
        # Adiciona à lista de tarefas em background
        self._background_tasks.add(task)
        
        # Log que a tarefa foi agendada
        logger.info(f"Agendada atualização de imagens em background para {title} (ID: {content_id})")
    
    async def _background_associate_content(self, content: Dict[str, Union[int, str]], page_data: Dict, portlet_index: int = 0):
        """
        Função em background para associar conteúdo ao portlet
        
        Args:
            content: Dicionário com detalhes do conteúdo
            page_data: Dados da página
            portlet_index: Índice do portlet a ser usado
        """
        try:
            logger.info(f"[BACKGROUND] Iniciando associação de conteúdo ao portlet {portlet_index}")
            
            # Tenta associar o conteúdo ao portlet
            association_success = await self.associate_content_with_page_portlet(
                content, page_data, portlet_index
            )
            
            if association_success:
                logger.info(f"[BACKGROUND] Conteúdo associado com sucesso ao portlet {portlet_index}")
            else:
                logger.warning(f"[BACKGROUND] Falha na associação de conteúdo ao portlet {portlet_index}")
        
        except Exception as e:
            error_msg = f"[BACKGROUND] Erro na associação de conteúdo: {str(e)}"
            logger.error(error_msg)
            self._log_error("Background Content Association", "", error_msg)
        finally:
            # Remove a tarefa da lista de tarefas em background
            if task := asyncio.current_task():
                self._background_tasks.discard(task)

    async def close(self):
        """Fecha recursos de forma eficiente, aguardando tarefas em background"""
        if not self.session:
            return
            
        try:
            # Lista de tarefas pendentes das diferentes classes
            background_tasks = []
            
            # Tarefas do próprio WebContentCreator
            if hasattr(self, '_background_tasks') and self._background_tasks:
                background_tasks.extend(list(self._background_tasks))
                
            # Tarefas do CollapseContentProcessor
            if hasattr(self, 'collapse_processor') and hasattr(self.collapse_processor, '_background_tasks'):
                background_tasks.extend(list(self.collapse_processor._background_tasks))
                
            # Tarefas do TabContentProcessor
            if hasattr(self, 'tab_processor') and hasattr(self.tab_processor, '_background_tasks'):
                background_tasks.extend(list(self.tab_processor._background_tasks))
                
            # Aguardar tarefas em background antes de fechar
            if background_tasks:
                logger.info(f"Aguardando {len(background_tasks)} tarefas em background concluírem...")
                
                # Esperar todas as tarefas com timeout para não bloquear indefinidamente
                done, pending = await asyncio.wait(
                    background_tasks, 
                    timeout=3000  # Timeout de 60 segundos
                )
                
                if pending:
                    logger.warning(f"{len(pending)} tarefas em background não concluídas no timeout")
                
                # Cancela as tarefas pendentes
                for task in pending:
                    task.cancel()
                    
                logger.info(f"{len(done)} tarefas em background concluídas com sucesso")
            
            # Fecha as sessões
            close_tasks = [
                self.session.close(),
                self.folder_creator.close(),
                self.document_creator.close()
            ]
            
            # Fecha processadores específicos se tiverem método close
            if hasattr(self, 'collapse_processor') and hasattr(self.collapse_processor, 'close'):
                close_tasks.append(self.collapse_processor.close())
                
            if hasattr(self, 'tab_processor') and hasattr(self.tab_processor, 'close'):
                close_tasks.append(self.tab_processor.close())
            
            # Fecha ContentUpdater se existir
            if hasattr(self, 'content_updater'):
                close_tasks.append(self.content_updater.close())
            
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