import asyncio
from asyncio.log import logger
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import traceback
from typing import List, Optional, Dict, Union
import aiohttp
from aiohttp import ClientTimeout, BasicAuth, TCPConnector
from dataclasses import dataclass
from bs4 import BeautifulSoup
from web_content_processor import ContentProcessor
from folder_creator import FolderCreator
from document_creator import DocumentCreator
from collapse_content_creator import CollapseContentProcessor
from web_content_cache import ContentCache
from web_content_mixed import MixedContentProcessor
from url_utils import UrlUtils

@dataclass
class ContentResponse:
    content_id: int
    is_new: bool
    error: Optional[str] = None

class WebContentCreator:
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
        self._setup_logging()
        self.content_processor = ContentProcessor(self)

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
        """Inicializa sessão HTTP com limites ampliados"""
        if self.session:
            await self.session.close()
        
        timeout = ClientTimeout(total=180, connect=30, sock_read=30)  # Reduzido para ser mais responsivo
        auth = BasicAuth(login=self.config.liferay_user, password=self.config.liferay_pass)
        
        self.session = aiohttp.ClientSession(
            auth=auth,
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'User-Agent': 'ContentMigrator/1.0'
            },
            timeout=timeout,
            connector=TCPConnector(
                ssl=False,
                limit=50,  # Aumentado de 10 para 50
                ttl_dns_cache=600,  # Aumentado de 300 para 600
                force_close=True
            )
        )
        
        await asyncio.gather(
            self.folder_creator.initialize_session(),
            self.document_creator.initialize_session()
        )


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
        """Wrapper para operações com retry otimizado"""
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return await operation(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))  # Reduzido de 2**attempt
                    continue
                raise last_error

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

            async with self.session.get(full_url, ssl=False) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch content: {response.status}")
                
                html = await response.text()
                # Parse HTML apenas uma vez
                soup = BeautifulSoup(html, 'html.parser')
                
                for selector in self.content_processor.CONTENT_SELECTORS:
                    content = soup.find(**{
                        'id' if selector['type'] == 'id' else 'class_': selector['value']
                    })
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

            url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{folder_id}/structured-contents"

            async def create_attempt():
                async with self.session.post(url, json=content_data) as response:
                    if response.status in (200, 201):
                        result = await response.json()
                        content_id = result.get('id')
                        content_key = result.get('key')
                        if content_id:
                            logger.info(f"Created content: {title} (ID: {content_id}, Key: {content_key})")
                            self.cache.add_content(title, content_id)
                            return {"id": int(content_id), "key": content_key}
                    
                    response_text = await response.text()
                    raise Exception(f"Failed to create content: {response.status} - {response_text}")

            return await self._retry_operation(create_attempt)
            
        except Exception as e:
            self._log_error("Content Creation", title, str(e))
            return {"id": 0, "key": ""}
    
    async def migrate_content(self, source_url: str, title: str, hierarchy: List[str]) -> Dict[str, Union[int, str]]:
        """Migra conteúdo e retorna tanto o ID quanto a key do conteúdo"""
        try:
            cached_content_id = self.cache.get_content(title)
            if cached_content_id:
                logger.info(f"Using cached content for {title}")
                # Buscar a key se estiver usando cache
                url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{cached_content_id}"
                async with self.session.get(url, ssl=False) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {"id": cached_content_id, "key": data.get('key')}
                    return {"id": cached_content_id, "key": str(cached_content_id)}

            # Primeiro busca os dados da página
            page_data = await self.find_page_by_title_or_id(title)
            logger.info(f"Found page data: {page_data}")

            # Cria recursos necessários em paralelo
            folder_id, folder_id_dl = await asyncio.gather(
                self.folder_creator.create_folder_hierarchy(hierarchy, title, 'journal'),
                self.folder_creator.create_folder_hierarchy(hierarchy, title, 'documents')
            )

            if not folder_id:
                raise Exception(f"Could not create/find folder for: {title}")

            # Verifica conteúdo existente
            existing_content = await self.find_existing_content(title, folder_id)
            if existing_content:
                if page_data:
                    await self.associate_content_with_page_portlet(existing_content, page_data)
                return existing_content

            # Processa o conteúdo
            process_result = await self.content_processor.fetch_and_process_content(source_url, folder_id_dl)
            if not process_result["success"]:
                raise Exception(process_result["error"])

            content_result = None
            content_results = []

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

            # Associa conteúdos à página
            if page_data:
                if content_results:
                    for result in content_results:
                        await self.associate_content_with_page_portlet(result['key'], page_data)
                else:
                    await self.associate_content_with_page_portlet(content_result, page_data)

            self.cache.add_content(title, content_result)
            return content_result

        except Exception as e:
            error_msg = f"Error during content migration: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self._log_error("Content Migration", source_url, error_msg, title, hierarchy)
            return {"id": 0, "key": ""}
    
    async def find_existing_content(self, title: str, folder_id: int) -> Optional[int]:
        """Busca conteúdo existente"""
        cached_id = self.cache.get_content(title)
        if cached_id:
            return cached_id

        if not self.session:
            await self.initialize_session()

        try:
            url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{folder_id}/structured-contents"
            params = {
                'filter': f"title eq '{title}'",
                'fields': 'id,title,key',
                'page': 1,
                'pageSize': 1
            }
            
            async with self.session.get(url, params=params, ssl=False) as response:
                if response.status == 200:
                    data = await response.json()
                    for content in data.get('items', []):
                        if content['title'].lower() == title.lower():
                            content_key = int(content['key'])
                            self.cache.add_content(title, content_key)
                            logger.info(f"Found existing content: {title} (ID: {content_key})")
                            return content_key
                            
            logger.info(f"No existing content found with title: {title}")
            return None
                    
        except Exception as e:
            self._log_error("Content Search", title, str(e))
            return None

    async def find_page_by_title_or_id(self, identifier: Union[str, int]) -> Optional[Dict]:
        """
        Busca uma página pelo título ou ID e obtém os detalhes através do rendered-page
        """
        try:
            if not self.session:
                await self.initialize_session()

            url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/site-pages"
            params = {
                'page': 1,
                'pageSize': 100,
                'search': identifier,
                'fields': 'id,title,friendlyUrlPath'
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    logger.error(f"Failed to search pages. Status: {response.status}")
                    return None
                    
                data = await response.json()
                items = data.get('items', [])
                
                # Procura correspondência exata primeiro
                page_data = None
                for item in items:
                    if item.get('title', '').lower() == str(identifier).lower():
                        page_data = item
                        break
                
                # Se não encontrou exata, tenta parcial
                if not page_data:
                    for item in items:
                        if str(identifier).lower() in item.get('title', '').lower():
                            page_data = item
                            break
                
                if not page_data:
                    return None

                # Busca rendered page
                friendly_url = page_data.get('friendlyUrlPath', '').strip('/')
                if not friendly_url:
                    return None

                rendered_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/site-pages/{friendly_url}/rendered-page"
                
                headers = {
                    'Accept': 'text/html'
                }
                
                async with self.session.get(rendered_url, headers=headers) as rendered_response:
                    if rendered_response.status == 200:
                        rendered_html = await rendered_response.text()
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(rendered_html, 'html.parser')

                        portlets = []
                        journal_portlets = soup.find_all(
                            lambda tag: tag.get('id', '').startswith('p_p_id_com_liferay_journal_content_web_portlet_JournalContentPortlet')
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
                                'portletId': 'com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_JournalCont_',
                                'articleId': ''
                            })

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
            return 'com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_JournalCont_'
            
        except Exception as e:
            logger.error(f"Error getting portlet instance: {str(e)}")
            return None

    async def associate_content_with_page_portlet(self, content: Union[Dict[str, Union[int, str]], str, int], page_data: Union[Dict, int, str]) -> bool:
        """
        Associa um conteúdo ao portlet Journal Content de uma página
        
        :param content: Pode ser um dicionário com 'id' e 'key', ou diretamente a key como string/int
        :param page_data: Dados da página ou identificador
        :return: Boolean indicando sucesso da operação
        """
        try:
            if not isinstance(page_data, dict):
                page_info = await self.find_page_by_title_or_id(page_data)
                if not page_info:
                    return False
            else:
                page_info = page_data
                
            portlet_id = await self.get_journal_portlet_instance(page_info)
            if not portlet_id:
                return False

            if not portlet_id.startswith('p_p_id_'):
                portlet_id = f'p_p_id_{portlet_id}'

            portlet_id = portlet_id.replace('p_p_id_', '')

            if portlet_id.endswith('_'):
                portlet_id = portlet_id[:-1]
            
            content_key = content.get('key') if isinstance(content, dict) else str(content)
            
            association_url = f"{self.config.liferay_url}/o/api-association-migrador/v1.0/journal-content/associate-article"

            params = {
                'plid': str(page_info['id']),
                'portletId': portlet_id,
                'articleId': content_key
            }

            async def associate_content():
                async with self.session.post(association_url, params=params, ssl=False) as response:
                    if response.status in (200, 201):
                        result = await response.json()
                        logger.info(f"Association result: {result}")
                        return result.get('status') == 'SUCCESS'
                    return False

            return await self._retry_operation(associate_content)
            
        except Exception as e:
            logger.error(f"Error associating content: {str(e)}")
            return False
    
    async def create_and_associate_content(self, source_url: str, title: str, hierarchy: List[str], 
                                        page_identifier: Union[str, int], page_friendly_url: str = None) -> ContentResponse:
        """
        Cria conteúdo e associa a um portlet específico da página
        
        :param source_url: URL do conteúdo fonte
        :param title: Título do conteúdo
        :param hierarchy: Hierarquia do conteúdo
        :param page_identifier: ID ou título da página
        :param page_friendly_url: URL amigável da página (opcional)
        :return: ContentResponse com status e detalhes
        """
        try:
            # Primeiro cria o conteúdo
            content_result = await self.migrate_content(source_url, title, hierarchy)
            if not content_result or not content_result['id']:
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
            return ContentResponse(content_id=0, is_new=False, error=error_msg)
    
    async def close(self):
        """Fecha recursos"""
        if not self.session:
            return
            
        try:
            await asyncio.gather(
                self.session.close(),
                self.folder_creator.close(),
                self.document_creator.close()
            )
        finally:
            self.session = None
            self.cache.clear_all()
