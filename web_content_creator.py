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

logger = logging.getLogger(__name__)

class WebContentCreator:
    def __init__(self, config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.folder_creator = FolderCreator(config)
        self.document_creator = DocumentCreator(config)
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
            }
        )
        await self.folder_creator.initialize_session()
        await self.document_creator.initialize_session()

    async def fetch_content(self, url: str) -> str:
        """Fetch HTML content from URL"""
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
            
            async with self.session.get(full_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    content = soup.find(id='conteudo')
                    
                    if not content:
                        content = (
                            soup.find(id='main-content') or 
                            soup.find(id='content') or 
                            soup.find(class_='content') or
                            soup.find('article')
                        )
                    
                    if content:
                        logger.info(f"Found content element: {content.name} (id={content.get('id', '')}, class={content.get('class', '')})")
                        return str(content)
                    else:
                        logger.error(f"No content element found in {full_url}")
                        return ''
                else:
                    logger.error(f"Failed to fetch content from {full_url}: {response.status}")
                    return ''
        except Exception as e:
            logger.error(f"Error fetching content from {url}: {str(e)}")
            logger.error(traceback.format_exc())
            return ''

    async def process_content(self, html_content: str, base_url: str, folder_id: Optional[int] = None) -> str:
        """
        Processa o conteúdo HTML:
        - Migra imagens e documentos usando DocumentCreator
        - Mantém URLs absolutas para wp-content e wp-conteudo
        - Converte outras URLs do mesmo domínio para relativas
        """
        if not html_content:
            return ""

        soup = BeautifulSoup(html_content, 'html.parser')
        base_domain = self.url_utils.extract_domain(base_url)

        # Processa todas as tags com atributos que podem conter URLs
        for tag in soup.find_all(['a', 'img', 'link', 'script']):
            # Lista de atributos para verificar em cada tag
            url_attrs = {
                'a': ['href'],
                'img': ['src', 'data-src'],
                'link': ['href'],
                'script': ['src']
            }

            # Processa os atributos relevantes para cada tag
            for attr in url_attrs.get(tag.name, []):
                url = tag.get(attr)
                if not url:
                    continue

                # Se for uma imagem ou documento com wp-content/wp-conteudo
                if tag.name == 'img' or any(wp_path in url for wp_path in ['/wp-conteudo', '/wp-content']):
                    # Constrói a URL completa se necessário
                    if not url.startswith('http'):
                        url = f"{base_domain}{url}"
                    
                    # Extrai a hierarquia atual da URL base
                    hierarchy = base_url.replace(base_domain, '').strip('/').split('/')
                    hierarchy_str = ' > '.join(hierarchy)
                    
                    # Migra o documento/imagem com informações da página
                    migrated_url = await self.document_creator.migrate_document(
                        doc_url=url,
                        folder_id=folder_id,
                        page_url=base_url,
                        hierarchy=f"Path: {hierarchy_str} (Folder ID: {folder_id})"
                    )
                    
                    if migrated_url:
                        tag[attr] = migrated_url
                        logger.info(f"Migrated document/image: {url} -> {migrated_url}")
                    continue

                # Para outras URLs do mesmo domínio, converte para relativa
                if base_domain in url:
                    path = url.split(base_domain)[-1]
                    tag[attr] = path
                    logger.info(f"Converted absolute URL to relative: {url} -> {path}")

        return str(soup)

    async def create_structured_content(self, title: str, html_content: str, folder_id: int) -> int:
        """Create structured content in Liferay"""
        if not self.session:
            await self.initialize_session()

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

        try:
            async with self.session.post(url, json=content_data) as response:
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
            async with self.session.get(url) as response:
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
            
            async with self.session.get(url) as response:
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

    async def migrate_content(self, source_url: str, title: str, hierarchy: List[str]) -> int:
        """
        Migra conteúdo para o Liferay e tenta associá-lo à página correspondente
        
        Args:
            source_url (str): URL da página fonte
            title (str): Título do conteúdo
            hierarchy (List[str]): Hierarquia de pastas
        
        Returns:
            int: ID do conteúdo criado ou 0 em caso de erro
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
                # Se existe conteúdo e tem page_id, associa
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

            # Processa o conteúdo (URLs relativas e documentos)
            processed_content = await self.process_content(html_content, source_url, folder_id_dl)
            if not processed_content:
                error_msg = f"Failed to process content for {source_url}"
                logger.error(error_msg)
                self._log_content_error(source_url, error_msg, title, hierarchy)
                return 0

            content_id = await self.create_structured_content(title, processed_content, folder_id)
            if not content_id:
                error_msg = "Failed to create structured content in Liferay"
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
            
            async with self.session.get(url) as response:
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
                
                async with self.session.post(prefs_url, params=prefs_params) as response:
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