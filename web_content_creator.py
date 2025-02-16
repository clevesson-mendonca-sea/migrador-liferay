import asyncio
from asyncio.log import logger
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import traceback
from typing import List, Optional, Dict
from urllib.parse import urlparse
import aiohttp
from bs4 import BeautifulSoup, Tag
import re
from functools import lru_cache
from dataclasses import dataclass
from aiohttp import ClientTimeout, BasicAuth, TCPConnector
from bs4 import BeautifulSoup
from url_utils import UrlUtils
from web_content_folder import FolderCreator
from document_creator import DocumentCreator
from collapse_content_creator import CollapseContentProcessor
from web_content_cache import ContentCache
from web_content_mixed import MixedContentProcessor


logger = logging.getLogger(__name__)

@dataclass
class ContentResponse:
    content_id: int
    is_new: bool
    error: Optional[str] = None

class WebContentCreator:
    CONTENT_SELECTORS = [
        {'type': 'id', 'value': 'conteudo'},
        {'type': 'class', 'value': 'corpo-principal'},
        {'type': 'class', 'value': 'col-md-8 col-md-offset-1'},
        {'type': 'class', 'value': 'col-md-8 col-md-offset-1 pull-right'},
        {'type': 'class', 'value': 'col-md-8'},
    ]
    
    BOOTSTRAP_PATTERNS = re.compile('|'.join([
        r'^col-\w+-\d+',
        r'^col-\w+-offset-\d+',
        r'^row',
        r'^container',
        r'^container-fluid',
        r'^offset-',
        r'^pull-',
        r'^push-',
        r'^col-xs-'
    ]))

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
        self.semaphore = asyncio.Semaphore(5)
        self.content_cache = {}
        self.page_cache = {}

    def _setup_logging(self):
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
        if self.session:
            await self.session.close()
        
        timeout = ClientTimeout(total=300, connect=60, sock_read=60)
        auth = BasicAuth(login=self.config.liferay_user, password=self.config
.liferay_pass)
        
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
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
                raise last_error

    @lru_cache(maxsize=1000)
    def _clean_url(self, url: str) -> str:
        """Clean and normalize URLs with caching"""
        if not url or url.startswith('/'):
            return url
        try:
            parsed = urlparse(url)
            path = parsed.path
            if parsed.query:
                path = f"{path}?{parsed.query}"
            if parsed.fragment:
                path = f"{path}#{parsed.fragment}"
            return f"/{path.lstrip('/')}"
        except Exception:
            return url

    async def fetch_content(self, url: str) -> str:
        """Optimized content fetching with caching"""
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
                soup = BeautifulSoup(html, 'html.parser')
                
                for selector in self.CONTENT_SELECTORS:
                    content = soup.find(**{
                        'id' if selector['type'] == 'id' else 'class_': selector['value']
                    })
                    if content:
                        cleaned_content = self._clean_content(str(content))
                        self.cache.add_url(url, cleaned_content)
                        return cleaned_content

                raise Exception("No valid content found")

        except Exception as e:
            self._log_error("Content Fetch", url, str(e))
            return ''

    def _clean_content(self, html_content: str) -> str:
        """Optimized content cleaning"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove date/time patterns
        if date_div := soup.find('div', style='font-size:14px;'):
            if re.search(r'\d{2}/\d{2}/\d{2}\s+às\s+\d{2}h\d{2}', date_div.text):
                date_div.decompose()
        
        # Fix email links and clean up in one pass
        for element in soup.find_all(['a', 'p', 'div']):
            if element.name == 'a':
                href = element.get('href', '')
                if href.startswith('/') and '@' in href:
                    element['href'] = f'mailto:{href.lstrip("/")}'
            elif element.name == 'p':
                if element.string and (element.string.isspace() or element.string == '\xa0'):
                    element.decompose()
            elif element.name == 'div' and 'margin-top-20' in element.get('class', []):
                element.decompose()
        
        return str(soup)

    async def process_content(self, html_content: str, base_url: str, folder_id: Optional[int] = None) -> str:
        """Process content with optimized URL handling"""
        if not html_content:
            return ""

        soup = BeautifulSoup(html_content, 'html.parser')
        base_domain = self.url_utils.extract_domain(base_url)

        # Process all tags in a single pass
        async def process_tags():
            tasks = []
            for tag in soup.find_all(['a', 'img', 'link', 'script']):
                attr = next((a for a in ['href', 'src', 'data-src'] if tag.get(a)), None)
                if attr and (url := tag.get(attr)):
                    tasks.append(self._process_url(tag, attr, url, base_domain, folder_id, base_url))
            
            return await asyncio.gather(*tasks) if tasks else []

        await process_tags()
        return str(soup)

    async def _process_url(self, tag: Tag, attr: str, url: str, base_domain: str, folder_id: int, base_url: str) -> None:
        """Process individual URLs with optimized caching"""
        async with self.semaphore:
            try:
                if cached_url := self.cache.get_url(url):
                    tag[attr] = cached_url
                    return

                if any(wp_path in url for wp_path in ['/wp-conteudo', '/wp-content']):
                    if not url.startswith(('http://', 'https://')):
                        url = f"{base_domain.rstrip('/')}/{url.lstrip('/')}"
                    
                    migrated_url = await self._retry_operation(
                        self.document_creator.migrate_document,
                        doc_url=url,
                        folder_id=folder_id,
                        page_url=base_url
                    )

                    if migrated_url:
                        relative_url = self._clean_url(migrated_url)
                        self.cache.add_url(url, relative_url)
                        tag[attr] = relative_url
                    else:
                        self.cache.mark_failed(url)
                else:
                    tag[attr] = self._clean_url(url)

            except Exception as e:
                self._log_error("URL Processing", url, str(e))
                self.cache.mark_failed(url)

    def _convert_to_relative(self, url: str) -> str:
        """Converte URLs para formato relativo"""
        if not url:
            return url

        if url.startswith('/'):
            return url

        try:
            parsed = urlparse(url)
            path = parsed.path
            
            # Adiciona query string e fragment se existirem
            if parsed.query:
                path = f"{path}?{parsed.query}"
            if parsed.fragment:
                path = f"{path}#{parsed.fragment}"
            
            return f"/{path.lstrip('/')}"
            
        except Exception as e:
            self._log_error("URL Conversion", url, str(e))
            return url

    def _clean_first_div_bootstrap(self, html_content: str) -> str:
        """Remove classes Bootstrap da primeira div de forma otimizada"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            first_div = soup.find('div')
            
            if not first_div:
                return html_content
                
            # Padrões de classes Bootstrap para remover
            bootstrap_patterns = '|'.join([
                r'^col-\w+-\d+',
                r'^col-\w+-offset-\d+',
                r'^row',
                r'^container',
                r'^container-fluid',
                r'^offset-',
                r'^pull-',
                r'^push-',
                r'^col-xs-'
            ])
            
            bootstrap_regex = re.compile(bootstrap_patterns)
            
            if 'class' in first_div.attrs:
                classes = first_div.get('class', [])
                if isinstance(classes, str):
                    classes = classes.split()
                
                # Filtra classes não-Bootstrap
                cleaned_classes = [
                    c for c in classes 
                    if not bootstrap_regex.match(c)
                ]
                
                if cleaned_classes:
                    first_div['class'] = cleaned_classes
                else:
                    del first_div['class']
            
            return str(soup)
            
        except Exception as e:
            self._log_error("Bootstrap Cleaning", html_content[:100], str(e))
            return html_content

    def _is_collapsible_content(self, html_content: str) -> bool:
        """Verifica se o conteúdo é colapsável de forma mais precisa"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Verifica panels com classe específica
            panels = soup.find_all('div', class_=['panel', 'panel-default'])
            if not panels:
                return False
            
            # Verifica estrutura de cada painel
            for panel in panels:
                # Busca elementos que indicam estrutura colapsável
                has_heading = bool(panel.find('div', class_='panel-heading'))
                has_collapse = bool(panel.find('div', class_='panel-collapse'))
                has_body = bool(panel.find('div', class_='panel-body'))
                has_title = bool(panel.find(['h4', 'h3'], class_='panel-title'))
                
                # Log para diagnóstico
                logger.debug(f"""
                    Panel structure:
                    - Has heading: {has_heading}
                    - Has collapse: {has_collapse}
                    - Has body: {has_body}
                    - Has title: {has_title}
                    - Classes: {panel.get('class', [])}
                """)
                
                # Se tem pelo menos a estrutura básica de um panel colapsável
                if (has_heading and (has_collapse or has_body)) or (has_title and has_body):
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking collapsible content: {str(e)}")
            return False
        
    async def create_structured_content(self, title: str, html_content: str, folder_id: int) -> int:
        """Cria conteúdo estruturado com retry e validação"""
        if not self.session:
            await self.initialize_session()

        try:
            # Prepara dados do conteúdo
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
                "title": title
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
            
    async def find_existing_content(self, title: str, folder_id: int) -> Optional[int]:
        """Busca conteúdo existente com cache"""
        # Verifica cache primeiro
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
        """Busca página por título com cache e paginação"""
        # Verifica cache
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
                        
                        # Verifica se há mais páginas
                        total_count = data.get('totalCount', 0)
                        if total_count > page * per_page:
                            return await fetch_pages(page + 1, per_page)
                return None

            return await self._retry_operation(fetch_pages)
                    
        except Exception as e:
            self._log_error("Page Search", title, str(e))
            return None

    async def migrate_content(self, source_url: str, title: str, hierarchy: List[str]) -> int:
        """Migra conteúdo processando tudo igualmente e alterando apenas a estrutura final"""
        try:
            # Verifica cache de conteúdo
            cached_content_id = self.cache.get_content(title)
            if cached_content_id:
                logger.info(f"Using cached content for {title} (ID: {cached_content_id})")
                return cached_content_id

            # Busca/cria recursos necessários em paralelo
            page_id, folder_id, folder_id_dl = await asyncio.gather(
                self.find_page_by_title(title),
                self.folder_creator.create_folder_hierarchy(
                    hierarchy=hierarchy,
                    final_title=title,
                    folder_type='journal'
                ),
                self.folder_creator.create_folder_hierarchy(
                    hierarchy=hierarchy,
                    final_title=title,
                    folder_type='documents'
                )
            )

            if not folder_id:
                raise Exception(f"Could not create/find folder for: {title}")

            # Verifica conteúdo existente
            existing_content_id = await self.find_existing_content(title, folder_id)
            if existing_content_id:
                if page_id:
                    await self.add_content_to_page(page_id, existing_content_id)
                return existing_content_id

            # Busca e processa o conteúdo inicial
            html_content = await self.fetch_content(source_url)
            if not html_content:
                raise Exception(f"No content found for {source_url}")

            # Processa o conteúdo (baixa arquivos, processa URLs, etc)
            processed_content = await self.process_content(html_content, source_url, folder_id_dl)
            if not processed_content:
                raise Exception(f"Failed to process content for {source_url}")
            
            # Limpa classes bootstrap
            cleaned_content = self._clean_first_div_bootstrap(processed_content)

            # Identifica o tipo de conteúdo
            is_fully_collapsible = self._is_collapsible_content(cleaned_content)
            
            # Verifica seções colapsáveis individuais
            soup = BeautifulSoup(cleaned_content, 'html.parser')
            has_some_collapsible = any(self._is_collapsible_content(str(elem)) for elem in soup.find_all('div'))

            content_id = None
            content_ids = []

            if is_fully_collapsible:
                # Conteúdo totalmente colapsável
                logger.info(f"Creating fully collapsible content for {title}")
                content_id = await self.collapse_processor.create_collapse_content(
                    self, title=title, html_content=cleaned_content, folder_id=folder_id
                )
            elif has_some_collapsible:
                # Conteúdo misto
                logger.info(f"Creating mixed content for {title}")
                content_ids = await self.mixed_processor.process_mixed_content(
                    self, title, cleaned_content, folder_id, folder_id_dl, source_url
                )
                if content_ids:
                    content_id = content_ids[0]
                else:
                    raise Exception(f"Failed to process mixed content for {source_url}")
            else:
                # Conteúdo regular
                logger.info(f"Creating regular content for {title}")
                content_id = await self.create_structured_content(
                    title, cleaned_content, folder_id
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

            # Cache e retorno
            self.cache.add_content(title, content_id)
            return content_id

        except Exception as e:
            error_msg = f"Error during content migration: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self._log_error("Content Migration", source_url, error_msg, title, hierarchy)
            return 0
        
    async def add_content_to_created_pages(self, content_mapping: Dict[str, int]):
        """Adiciona conteúdo às páginas com retry e cache"""
        if not self.session:
            await self.initialize_session()

        try:
            # Busca páginas em lotes para melhor performance
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
                        for i, result in enumerate(results):
                            if isinstance(result, Exception):
                                logger.error(f"Error adding content to page: {str(result)}")
                    
                    page += 1

            await process_pages()
                
        except Exception as e:
            logger.error(f"Error adding content to pages: {str(e)}")
            logger.error(traceback.format_exc())

    async def add_content_to_page(self, page_id: int, content_id: int) -> bool:
        """Adiciona conteúdo à página com retry e verificação"""
        if not self.session:
            await self.initialize_session()
                
        try:
            # Verifica se o portlet já existe
            portlet_id = f"com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_{content_id}"
            
            async def check_existing_portlet():
                check_url = f"{self.config.liferay_url}/api/jsonws/journalcontentportlet/get-portlet-preferences"
                check_params = {
                    "plid": str(page_id),
                    "portletId": portlet_id
                }
                
                async with self.session.get(check_url, params=check_params, ssl=False) as response:
                    return response.status == 200

            # Se já existe, retorna sucesso
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

    async def close(self):
        """Clean shutdown of resources"""
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
            self.content_cache.clear()
            self.page_cache.clear()
          