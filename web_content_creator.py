import asyncio
from datetime import datetime
import logging
import json
import traceback
from typing import List, Optional
import aiohttp
from bs4 import BeautifulSoup
from url_utils import UrlUtils
from web_content_folder import FolderCreator
from document_creator import DocumentCreator
from collapse_content_creator import CollapseContentProcessor

logger = logging.getLogger(__name__)

class WebContentCreator:
    def __init__(self, config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.folder_creator = FolderCreator(config)
        self.document_creator = DocumentCreator(config)
        self.collapse_processor = CollapseContentProcessor(config)
        self.base_domain = ""
        self.url_utils = UrlUtils()
        self.error_log_file = "content_migration_errors.txt"

    def _log_content_error(self, url: str, error_msg: str, title: str, hierarchy: List[str]):
        """
        Registra erros de migração de conteúdo em um arquivo de texto
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        hierarchy_str = ' > '.join(hierarchy)
        
        error_entry = (
            f"\n=== Content Error Log Entry: {timestamp} ===\n"
            f"Page URL: {url}\n"
            f"Title: {title}\n"
            f"Hierarchy: {hierarchy_str}\n"
            f"Error: {error_msg}\n"
            f"{'=' * 50}\n"
        )
        
        try:
            with open(self.error_log_file, "a", encoding="utf-8") as f:
                f.write(error_entry)
        except Exception as e:
            logger.error(f"Erro ao salvar log de erro de conteúdo: {str(e)}")
            
    async def initialize_session(self):
        """Initialize HTTP session"""
        auth = aiohttp.BasicAuth(
            login=self.config.liferay_user,
            password=self.config.liferay_pass
        )
        self.session = aiohttp.ClientSession(
            auth=auth,
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            connector=aiohttp.TCPConnector(ssl=False)
        )
        await self.folder_creator.initialize_session()
        await self.document_creator.initialize_session()

    async def fetch_content(self, url: str) -> str:
        """
        Fetch HTML content from URL using multiple possible selectors
        """
        if not self.session:
            await self.initialize_session()

        try:
            # Se ainda não temos um domínio base, tenta extrair desta URL
            if not self.base_domain:
                self.base_domain = self.url_utils.extract_domain(url)
                logger.info(f"Base domain set to: {self.base_domain}")

            # Constrói a URL completa
            full_url = self.url_utils.build_url(url, self.base_domain)
            if not full_url:
                logger.error(f"Invalid URL: {url}")
                return ''

            logger.info(f"Fetching content from: {full_url}")
            
            async with self.session.get(full_url, ssl=False) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Lista de seletores para procurar, em ordem de prioridade
                    selectors = [
                        {'type': 'id', 'value': 'conteudo'},
                        {'type': 'class', 'value': 'corpo-principal'},
                        {'type': 'class', 'value': 'col-md-8 col-md-offset-1'},
                        {'type': 'class', 'value': 'col-md-8'},
                    ]
                    
                    # Tenta encontrar o conteúdo usando cada seletor
                    for selector in selectors:
                        content = None
                        if selector['type'] == 'id':
                            content = soup.find(id=selector['value'])
                        elif selector['type'] == 'class':
                            # Trata classes múltiplas
                            classes = selector['value'].split()
                            content = soup.find(class_=lambda x: x and all(c in x.split() for c in classes))
                        elif selector['type'] == 'tag':
                            content = soup.find(selector['value'])
                            
                        if content:
                            logger.info(f"Found content using selector: {selector['type']}={selector['value']}")
                            
                            # Verifica se o conteúdo não está vazio
                            if content.get_text(strip=True):
                                # Remove elementos indesejados antes de retornar
                                for element in content.find_all(['script', 'style', 'iframe']):
                                    element.decompose()
                                    
                                return str(content)
                            else:
                                logger.info(f"Content found with {selector['type']}={selector['value']} was empty, trying next selector")
                    
                    logger.error(f"No valid content found in {full_url} using any selector")
                    return ''
                else:
                    logger.error(f"Failed to fetch content from {full_url}: {response.status}")
                    return ''
                    
        except Exception as e:
            logger.error(f"Error fetching content from {url}: {str(e)}")
            logger.error(traceback.format_exc())
            return ''

    def _clean_content(self, content_element: BeautifulSoup) -> BeautifulSoup:
        """
        Limpa e formata o conteúdo encontrado
        """
        if not content_element:
            return content_element
            
        # Remove elementos indesejados
        for element in content_element.find_all(['script', 'style', 'iframe']):
            element.decompose()
            
        # Remove atributos desnecessários
        for tag in content_element.find_all(True):
            attrs_to_remove = [attr for attr in tag.attrs if attr.startswith('on')]
            for attr in attrs_to_remove:
                del tag[attr]
                
        return content_element

    async def process_content(self, html_content: str, base_url: str, folder_id: Optional[int] = None) -> str:
        """
        Processa o conteúdo HTML:
        - Migra imagens e documentos usando DocumentCreator
        - Remove atributos srcset de imagens
        - Mantém URLs absolutas para wp-content e wp-conteudo
        - Converte outras URLs do mesmo domínio para relativas
        """
        if not html_content:
            return ""

        soup = BeautifulSoup(html_content, 'html.parser')
        base_domain = self.url_utils.extract_domain(base_url)

        # Remove atributos srcset e sizes de todas as imagens
        for img in soup.find_all('img'):
            if 'srcset' in img.attrs:
                del img['srcset']
            if 'sizes' in img.attrs:
                del img['sizes']

        # Processa todas as tags com atributos que podem conter URLs
        for tag in soup.find_all(['a', 'img', 'link', 'script']):
            url_attrs = {
                'a': ['href'],
                'img': ['src', 'data-src'],
                'link': ['href'],
                'script': ['src']
            }

            for attr in url_attrs.get(tag.name, []):
                url = tag.get(attr)
                if not url:
                    continue

                # Se for uma imagem ou documento com wp-content/wp-conteudo
                if tag.name == 'img' or any(wp_path in url for wp_path in ['/wp-conteudo', '/wp-content']):
                    # Constrói a URL completa se necessário
                    if not url.startswith('http'):
                        url = f"{base_domain}{url}"
                    
                    hierarchy = base_url.replace(base_domain, '').strip('/').split('/')
                    hierarchy_str = ' > '.join(hierarchy)
                    
                    try:
                        # Primeira tentativa de migração
                        migrated_url = await self.document_creator.migrate_document(
                            doc_url=url,
                            folder_id=folder_id,
                            page_url=base_url,
                            hierarchy=f"Path: {hierarchy_str} (Folder ID: {folder_id})"
                        )

                        if not migrated_url:
                            # Se falhou, tenta buscar URL existente
                            existing_url = await self.find_existing_document_url(url, folder_id)
                            if existing_url:
                                migrated_url = existing_url
                                logger.info(f"Found existing document: {url} -> {existing_url}")

                        if migrated_url:
                            tag[attr] = migrated_url
                            logger.info(f"Updated document/image URL: {url} -> {migrated_url}")
                        else:
                            logger.error(f"Failed to process document/image: {url}")
                    except Exception as e:
                        logger.error(f"Error processing URL {url}: {str(e)}")
                    continue

                # Para outras URLs do mesmo domínio, converte para relativa
                if base_domain in url:
                    path = url.split(base_domain)[-1]
                    tag[attr] = path
                    logger.info(f"Converted absolute URL to relative: {url} -> {path}")

        return str(soup)

    async def find_existing_document_url(self, original_url: str, folder_id: Optional[int]) -> Optional[str]:
        """
        Busca por um documento existente no Liferay baseado na URL original
        """
        if not self.session:
            await self.initialize_session()

        try:
            # Constrói a URL para buscar documentos na pasta
            if folder_id:
                url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{folder_id}/documents"
            else:
                url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/documents"

            async with self.session.get(url, ssl=False) as response:
                if response.status == 200:
                    data = await response.json()
                    for doc in data.get('items', []):
                        # Verifica se a descrição contém a URL original
                        if doc.get('description') and original_url in doc['description']:
                            return doc.get('contentUrl')
                
                return None

        except Exception as e:
            logger.error(f"Error searching for existing document: {str(e)}")
            return None
    
    def _clean_first_div_bootstrap(self, html_content: str) -> str:
        """
        Remove classes Bootstrap apenas da primeira div do conteúdo
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Encontra a primeira div
            first_div = soup.find('div')
            if not first_div:
                return html_content
                
            # Padrões de classes Bootstrap para remover
            bootstrap_patterns = [
                r'^col-\w+-\d+',  # col-md-8, col-lg-6, etc
                r'^col-\w+-offset-\d+',  # col-md-offset-1, etc
                r'^row',
                r'^container',
                r'^container-fluid',
                r'^offset-',
                r'^pull-',  # pull-right, pull-left
                r'^push-',
                r'^col-xs-'  # col-xs-12, etc
            ]
            
            import re
            bootstrap_regex = '|'.join(bootstrap_patterns)
            
            if 'class' in first_div.attrs:
                original_classes = first_div.get('class', [])
                if isinstance(original_classes, str):
                    original_classes = original_classes.split()
                
                # Filtra classes que não correspondem aos padrões Bootstrap
                cleaned_classes = [
                    c for c in original_classes 
                    if not re.match(bootstrap_regex, c)
                ]
                
                if cleaned_classes:
                    first_div['class'] = cleaned_classes
                else:
                    del first_div['class']
                    
                logger.debug(f"Cleaned first div classes: {original_classes} -> {cleaned_classes}")
            
            return str(soup)
            
        except Exception as e:
            logger.error(f"Error cleaning first div Bootstrap classes: {str(e)}")
            logger.error(traceback.format_exc())
            return html_content

    async def create_structured_content(self, title: str, html_content: str, folder_id: int) -> int:
        """Create structured content in Liferay with cleaned HTML content"""
        if not self.session:
            await self.initialize_session()

        # Limpa apenas a primeira div do conteúdo
        cleaned_content = self._clean_first_div_bootstrap(html_content)

        content_data = {
            "contentStructureId": self.config.content_structure_id,
            "contentFields": [
                {
                    "contentFieldValue": {
                        "data": cleaned_content
                    },
                    "name": "content"
                }
            ],
            "structuredContentFolderId": folder_id,
            "title": title
        }

        url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{folder_id}/structured-contents"

        try:
            async with self.session.post(url, json=content_data, ssl=False) as response:
                response_text = await response.text()
                if response.status in (200, 201):
                    result = json.loads(response_text)
                    content_id = result.get('id')
                    if content_id:
                        logger.info(f"Created structured content: {title} (ID: {content_id})")
                        return int(content_id)
                    
                logger.error(f"Failed to create structured content: {title}")
                logger.error(f"Response: {response_text}")
                
        except Exception as e:
            logger.error(f"Error creating structured content {title}: {str(e)}")
            logger.error(traceback.format_exc())
        
        return 0
    
    async def find_existing_content(self, title: str, folder_id: int) -> Optional[int]:
        """Procura por um conteúdo existente com o mesmo título na pasta"""
        if not self.session:
            await self.initialize_session()

        url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{folder_id}/structured-contents"
        
        try:
            async with self.session.get(url, ssl=False) as response:
                if response.status == 200:
                    data = await response.json()
                    for content in data.get('items', []):
                        if content['title'].lower() == title.lower():
                            logger.info(f"Found existing content: {title} (ID: {content['id']})")
                            return int(content['id'])
                            
            logger.info(f"No existing content found with title: {title}")
            return None
                    
        except Exception as e:
            logger.error(f"Error searching for existing content: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    async def find_page_by_title(self, title: str) -> Optional[int]:
        """
        Busca uma página pelo título e retorna seu ID.
        
        Args:
            title (str): Título da página
            
        Returns:
            Optional[int]: ID da página se encontrada, None caso contrário
        """
        if not self.session:
            await self.initialize_session()

        try:
            url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/site-pages"
            
            async with self.session.get(url, ssl=False) as response:
                if response.status == 200:
                    data = await response.json()
                    for page in data.get('items', []):
                        if page.get('title', '').lower() == title.lower():
                            page_id = page.get('id')
                            logger.info(f"Found page: {title} (ID: {page_id})")
                            return int(page_id)
                            
                    logger.info(f"No page found with title: {title}")
                    return None
                        
                logger.error(f"Failed to fetch pages: {response.status}")
                return None
                        
        except Exception as e:
            logger.error(f"Error searching for page: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    def _is_collapsible_content(self, html_content: str) -> bool:
        """
        Verifica se o conteúdo é do tipo colapsável baseado na estrutura HTML
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Verifica se tem painéis colapsáveis
            panels = soup.find_all('div', class_='panel-default')
            if not panels:
                return False
                
            # Verifica se tem estrutura típica de colapsável
            for panel in panels:
                has_heading = panel.find('div', class_='panel-heading')
                has_collapse = panel.find('div', class_='panel-collapse')
                has_body = panel.find('div', class_='panel-body')
                
                if has_heading and (has_collapse or has_body):
                    return True
                    
            return False
            
        except Exception as e:
            logger.error(f"Error checking if content is collapsible: {str(e)}")
            return False

    async def migrate_content(self, source_url: str, title: str, hierarchy: List[str]) -> int:
        """
        Migra conteúdo para o Liferay, detectando se é colapsável ou não
        """
        try:
            # Primeiro busca o page_id pelo título
            page_id = await self.find_page_by_title(title)
            if page_id:
                logger.info(f"Found matching page for content {title} (Page ID: {page_id})")
            else:
                logger.info(f"No matching page found for content {title}")

            folder_id = await self.folder_creator.create_folder_hierarchy(
                hierarchy=hierarchy,
                final_title=title,
                folder_type='journal'
            )
            
            folder_id_dl = await self.folder_creator.create_folder_hierarchy(
                hierarchy=hierarchy,
                final_title=title,
                folder_type='documents'
            )
            
            if not folder_id:
                error_msg = f"Could not find or create folder for: {title}"
                logger.error(error_msg)
                self._log_content_error(source_url, error_msg, title, hierarchy)
                return 0

            # Verifica se já existe um conteúdo com esse título
            existing_content_id = await self.find_existing_content(title, folder_id)
            if existing_content_id:
                logger.info(f"Content already exists: {title} (ID: {existing_content_id})")
                if page_id:
                    success = await self.add_content_to_page(page_id, existing_content_id)
                    if success:
                        logger.info(f"Existing content {existing_content_id} associated with page {page_id}")
                    else:
                        logger.error(f"Failed to associate existing content {existing_content_id} with page {page_id}")
                return existing_content_id

            # Busca o conteúdo original
            html_content = await self.fetch_content(source_url)
            if not html_content:
                error_msg = f"No content found for {source_url}"
                logger.error(error_msg)
                self._log_content_error(source_url, error_msg, title, hierarchy)
                return 0

            # Verifica se é conteúdo colapsável
            is_collapsible = self._is_collapsible_content(html_content)
            
            if is_collapsible:
                logger.info(f"Detected collapsible content for {title}")
                content_id = await self.collapse_processor.create_collapse_content(
                    self, 
                    title=title,
                    html_content=html_content,
                    folder_id=folder_id
                )
            else:
                # Processa como conteúdo normal
                processed_content = await self.process_content(html_content, source_url, folder_id_dl)
                if not processed_content:
                    error_msg = f"Failed to process content for {source_url}"
                    logger.error(error_msg)
                    self._log_content_error(source_url, error_msg, title, hierarchy)
                    return 0

                content_id = await self.create_structured_content(title, processed_content, folder_id)

            if not content_id:
                error_msg = "Failed to create content in Liferay"
                logger.error(error_msg)
                self._log_content_error(source_url, error_msg, title, hierarchy)
                return 0

            # Se encontrou uma página correspondente, associa o conteúdo
            if page_id:
                success = await self.add_content_to_page(page_id, content_id)
                if success:
                    logger.info(f"New content {content_id} associated with page {page_id}")
                else:
                    logger.error(f"Failed to associate new content {content_id} with page {page_id}")

            return content_id

        except Exception as e:
            error_msg = f"Unexpected error during content migration: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self._log_content_error(source_url, error_msg, title, hierarchy)
            return 0
        
    async def add_content_to_created_pages(self, content_mapping):
        """
        Adiciona conteúdo às páginas criadas no Liferay.
        
        Args:
            content_mapping (dict): Mapeamento de títulos de páginas para conteúdos migrados
        """
        if not self.session:
            await self.initialize_session()

        try:
            # Busca todas as páginas do site
            url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/site-pages"
            
            async with self.session.get(url, ssl=False) as response:
                if response.status == 200:
                    pages_data = await response.json()
                    
                    for page in pages_data.get('items', []):
                        page_title = page.get('name', '')
                        page_id = page.get('id')
                        
                        # Verifica se o título da página tem um conteúdo correspondente
                        if page_title in content_mapping:
                            content_id = content_mapping[page_title]
                            
                            # Adiciona o conteúdo à página
                            await self.add_content_to_page(page_id, content_id)
                else:
                    logger.error(f"Falha ao buscar páginas: {response.status}")
        
        except Exception as e:
            logger.error(f"Erro ao adicionar conteúdo às páginas: {e}")
            traceback.print_exc()

    async def add_content_to_page(self, page_id: int, content_id: int) -> bool:
            """
            Adiciona um Journal Content Portlet a uma página existente
            
            Args:
                page_id (int): ID da página
                content_id (int): ID do Web Content
            """
            if not self.session:
                await self.initialize_session()
                
            try:
                # Cria o portlet
                portlet_id = f"com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_{content_id}"
                
                # Configura as preferências do portlet
                prefs_url = f"{self.config.liferay_url}/api/jsonws/journalcontentportlet/add-portlet-preferences"
                prefs_params = {
                    "plid": str(page_id),
                    "portletId": portlet_id,
                    "articleId": str(content_id),
                    "groupId": str(self.config.site_id)
                }
                
                async with self.session.post(prefs_url, params=prefs_params, ssl=False) as response:
                    if response.status == 200:
                        logger.info(f"Conteúdo {content_id} adicionado à página {page_id}")
                        return True
                    else:
                        logger.error(f"Falha ao configurar portlet: {await response.status()}")
                        return False
                        
            except Exception as e:
                logger.error(f"Erro ao adicionar conteúdo à página: {str(e)}")
                logger.error(traceback.format_exc())
                return False
        
    async def close(self):
        """Close all sessions"""
        if self.session:
            await self.session.close()
        await self.folder_creator.close()
        await self.document_creator.close()