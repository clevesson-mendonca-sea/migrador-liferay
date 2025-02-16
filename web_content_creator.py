import asyncio
from asyncio.log import logger
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import traceback
from typing import List, Optional, Dict
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
        """Inicializa sessão HTTP"""
        if self.session:
            await self.session.close()
        
        timeout = ClientTimeout(total=300, connect=60, sock_read=60)
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
                limit=10,
                ttl_dns_cache=300,
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
        """Wrapper para operações com retry"""
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return await operation(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise last_error

    async def fetch_content(self, url: str) -> str:
        """Busca conteúdo da URL"""
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
                for selector in self.content_processor.CONTENT_SELECTORS:
                    content = BeautifulSoup(html, 'html.parser').find(**{
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

    async def create_structured_content(self, title: str, html_content: str, folder_id: int) -> int:
        """Cria conteúdo estruturado"""
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
                        if content_id:
                            logger.info(f"Created content: {title} (ID: {content_id})")
                            self.cache.add_content(title, content_id)
                            return int(content_id)
                    
                    response_text = await response.text()
                    raise Exception(f"Failed to create content: {response.status} - {response_text}")

            return await self._retry_operation(create_attempt)
            
        except Exception as e:
            self._log_error("Content Creation", title, str(e))
            return 0

    async def migrate_content(self, source_url: str, title: str, hierarchy: List[str]) -> int:
        """Migra conteúdo"""
        try:
            cached_content_id = self.cache.get_content(title)
            if cached_content_id:
                logger.info(f"Using cached content for {title}")
                return cached_content_id

            # Cria recursos necessários em paralelo
            page_id, folder_id, folder_id_dl = await asyncio.gather(
                self.find_page_by_title(title),
                self.folder_creator.create_folder_hierarchy(hierarchy, title, 'journal'),
                self.folder_creator.create_folder_hierarchy(hierarchy, title, 'documents')
            )

            if not folder_id:
                raise Exception(f"Could not create/find folder for: {title}")

            # Verifica conteúdo existente
            existing_content_id = await self.find_existing_content(title, folder_id)
            if existing_content_id:
                if page_id:
                    await self.add_content_to_page(page_id, existing_content_id)
                return existing_content_id

            # Processa o conteúdo
            process_result = await self.content_processor.fetch_and_process_content(source_url, folder_id_dl)
            if not process_result["success"]:
                raise Exception(process_result["error"])

            content_id = None
            content_ids = []

            if process_result["is_collapsible"]:
                logger.info(f"Creating collapsible content for {title}")
                content_id = await self.collapse_processor.create_collapse_content(
                    self, title, process_result["content"], folder_id
                )
            elif process_result["has_mixed_content"]:
                logger.info(f"Creating mixed content for {title}")
                content_ids = await self.mixed_processor.process_mixed_content(
                    self, title, process_result["content"], folder_id, folder_id_dl, source_url
                )
                content_id = content_ids[0] if content_ids else None
            else:
                logger.info(f"Creating regular content for {title}")
                content_id = await self.create_structured_content(
                    title, process_result["content"], folder_id
                )

            if not content_id:
                raise Exception("Failed to create content in Liferay")

            # Associa conteúdos à página
            if page_id:
                if content_ids:
                    for cid in content_ids:
                        await self.add_content_to_page(page_id, cid)
                else:
                    await self.add_content_to_page(page_id, content_id)

            self.cache.add_content(title, content_id)
            return content_id

        except Exception as e:
            error_msg = f"Error during content migration: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self._log_error("Content Migration", source_url, error_msg, title, hierarchy)
            return 0

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
                'fields': 'id,title',
                'page': 1,
                'pageSize': 1
            }
            
            async with self.session.get(url, params=params, ssl=False) as response:
                if response.status == 200:
                    data = await response.json()
                    for content in data.get('items', []):
                        if content['title'].lower() == title.lower():
                            content_id = int(content['id'])
                            self.cache.add_content(title, content_id)
                            logger.info(f"Found existing content: {title} (ID: {content_id})")
                            return content_id
                            
            logger.info(f"No existing content found with title: {title}")
            return None
                    
        except Exception as e:
            self._log_error("Content Search", title, str(e))
            return None

    async def find_page_by_title(self, title: str) -> Optional[int]:
        """Busca página por título"""
        cached_id = self.cache.get_page(title)
        if cached_id:
            return cached_id

        if not self.session:
            await self.initialize_session()
                
        try:
            url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/site-pages"
            
            async def fetch_pages(page=1, per_page=100):
                params = {
                    'page': page,
                    'pageSize': per_page,
                    'filter': f"title eq '{title}'"
                }
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        for page in data.get('items', []):
                            if page.get('title', '').lower() == title.lower():
                                page_id = int(page['id'])
                                self.cache.add_page(title, page_id)
                                return page_id
                        
                        total_count = data.get('totalCount', 0)
                        if total_count > page * per_page:
                            return await fetch_pages(page + 1, per_page)
                return None

            return await self._retry_operation(fetch_pages)
                    
        except Exception as e:
            self._log_error("Page Search", title, str(e))
            return None

    async def add_content_to_page(self, page_id: int, content_id: int) -> bool:
        """Adiciona conteúdo à página"""
        if not self.session:
            await self.initialize_session()
                
        try:
            portlet_id = f"com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_{content_id}"
            
            # Verifica se o portlet já existe
            async def check_existing_portlet():
                check_url = f"{self.config.liferay_url}/api/jsonws/journalcontentportlet/get-portlet-preferences"
                check_params = {
                    "plid": str(page_id),
                    "portletId": portlet_id
                }
                
                async with self.session.get(check_url, params=check_params, ssl=False) as response:
                    return response.status == 200

            if await self._retry_operation(check_existing_portlet):
                logger.info(f"Portlet already exists for content {content_id} on page {page_id}")
                return True

            # Configura novo portlet
            prefs_url = f"{self.config.liferay_url}/api/jsonws/journalcontentportlet/add-portlet-preferences"
            prefs_params = {
                "plid": str(page_id),
                "portletId": portlet_id,
                "articleId": str(content_id),
                "groupId": str(self.config.site_id)
            }
            
            async def add_portlet():
                async with self.session.post(prefs_url, params=prefs_params, ssl=False) as response:
                    if response.status == 200:
                        logger.info(f"Added content {content_id} to page {page_id}")
                        return True
                    else:
                        response_text = await response.text()
                        raise Exception(f"Failed to configure portlet: {response.status} - {response_text}")

            return await self._retry_operation(add_portlet)
                        
        except Exception as e:
            logger.error(f"Error adding content to page: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    async def add_content_to_created_pages(self, content_mapping: Dict[str, int]):
        """Adiciona conteúdo a múltiplas páginas"""
        if not self.session:
            await self.initialize_session()

        try:
            async def fetch_pages_batch(page=1, size=100):
                url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/site-pages"
                params = {'page': page, 'pageSize': size}
                
                async with self.session.get(url, params=params, ssl=False) as response:
                    if response.status == 200:
                        return await response.json()
                    return None

            async def process_pages():
                page = 1
                while True:
                    data = await fetch_pages_batch(page)
                    if not data or not data.get('items'):
                        break

                    tasks = []
                    for page_info in data['items']:
                        page_title = page_info.get('title', '')
                        page_id = page_info.get('id')
                        
                        if page_title in content_mapping:
                            content_id = content_mapping[page_title]
                            tasks.append(self.add_content_to_page(page_id, content_id))
                    
                    if tasks:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        for result in results:
                            if isinstance(result, Exception):
                                logger.error(f"Error adding content to page: {str(result)}")
                    
                    page += 1

            await process_pages()
                
        except Exception as e:
            logger.error(f"Error adding content to pages: {str(e)}")
            logger.error(traceback.format_exc())

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
