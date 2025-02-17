import json
import logging
import traceback
from typing import List, Optional, Dict, Any, Set
import aiohttp
from urllib.parse import urljoin, urlparse, unquote
from datetime import datetime
from bs4 import BeautifulSoup
import asyncio
from dataclasses import dataclass
from document_cache import DocumentCache

@dataclass
class MigrationConfig:
    liferay_url: str
    liferay_user: str
    liferay_pass: str
    site_id: str

logger = logging.getLogger(__name__)

class DocumentCreator:
    SUPPORTED_MIME_TYPES = {
        '.pdf': 'application/pdf',
        '.doc': 'application/msword',
        '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        '.xls': 'application/vnd.ms-excel',
        '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.ppt': 'application/vnd.ms-powerpoint',
        '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        '.txt': 'text/plain',
        '.csv': 'text/csv',
        '.json': 'application/json',
        '.xml': 'application/xml',
        '.html': 'text/html',
        '.htm': 'text/html',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml'
    }

    def __init__(self, config):
        self.config = config
        self.session = None
        self.cache = DocumentCache()
        self.semaphore = asyncio.Semaphore(10)  # Aumentado para 10 requisições simultâneas
        self.batch_size = 50  # Processa URLs em lotes de 50
        self._initialize_logging()

    def _initialize_logging(self):
        """Configure detailed logging"""
        if not logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

    def _log_migration_error(self, doc_url: str, error_msg: str, page_url: str, hierarchy: str):
        """Registra erros de migração em arquivo separado"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        error_entry = (
            f"\n=== Error Log Entry: {timestamp} ===\n"
            f"Page URL: {page_url}\n"
            f"Hierarchy: {hierarchy}\n"
            f"Document URL: {doc_url}\n"
            f"Error: {error_msg}\n"
            f"{'=' * 50}\n"
        )
        
        try:
            with open(self.error_log_file, "a", encoding="utf-8") as f:
                f.write(error_entry)
        except Exception as e:
            logger.error(f"Erro ao salvar log de erro: {str(e)}")

    async def initialize_session(self):
        """Initialize HTTP session"""
        if self.session:
            await self.session.close()
        
        auth = aiohttp.BasicAuth(
            login=self.config.liferay_user,
            password=self.config.liferay_pass
        )
        
        timeout = aiohttp.ClientTimeout(total=60)
        
        self.session = aiohttp.ClientSession(
            auth=auth,
            timeout=timeout,
            headers={
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            connector=aiohttp.TCPConnector(ssl=False, limit=10)
        )

    def _get_mime_type(self, filename: str) -> str:
        """Determina o MIME type pelo nome do arquivo"""
        ext = '.' + filename.split('.')[-1].lower() if '.' in filename else ''
        return self.SUPPORTED_MIME_TYPES.get(ext, 'application/octet-stream')

    async def _collect_urls(self, page_url: str) -> Set[str]:
        urls_to_process = set()
        
        async with self.session.get(page_url) as response:
            if response.status != 200:
                return urls_to_process
                
            html_content = await response.text()
            soup = BeautifulSoup(html_content, 'html.parser')
            content_div = soup.find(class_='paginas-internas')
            
            if not content_div:
                return urls_to_process

            # Processa links e imagens
            for element in content_div.find_all(['a', 'img']):
                url = element.get('href') or element.get('src')
                if url:
                    absolute_url = urljoin(page_url, url)
                    if self._is_valid_file_url(absolute_url):
                        urls_to_process.add(absolute_url)
                        
            # Processa srcset
            for img in content_div.find_all('img'):
                srcset = img.get('srcset')
                if srcset:
                    for url in [u.strip().split(' ')[0] for u in srcset.split(',')]:
                        absolute_url = urljoin(page_url, url)
                        if self._is_valid_file_url(absolute_url):
                            urls_to_process.add(absolute_url)
                            
        return urls_to_process

    async def _process_url_batch(self, urls: List[str], folder_id: Optional[int], page_url: str) -> List[str]:
        tasks = []
        for url in urls:
            if not self.cache.is_processed(url) and not self.cache.is_failed(url):
                tasks.append(self._process_single_url(url, folder_id, page_url))
                
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, str)]

    async def _process_single_url(self, url: str, folder_id: Optional[int], page_url: str) -> Optional[str]:
        async with self.semaphore:
            try:
                friendly_url = await self.migrate_document(url, folder_id, page_url)
                if friendly_url:
                    return friendly_url
                return None
            except Exception as e:
                logger.error(f"Erro ao processar URL {url}: {str(e)}")
                return None

    def _extract_filename(self, url: str, content_type: str = '') -> str:
        """Extrai um nome de arquivo válido da URL"""
        try:
            parsed_url = urlparse(url)
            path = unquote(parsed_url.path)
            filename = path.rstrip('/').split('/')[-1]
            
            if not filename or '.' not in filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                hostname = parsed_url.hostname.split('.')[0] if parsed_url.hostname else 'download'
                ext = self._get_extension_from_content_type(content_type)
                filename = f"{hostname}_{timestamp}{ext}"
                
            return self._sanitize_filename(filename)
            
        except Exception as e:
            logger.error(f"Erro ao extrair nome do arquivo: {str(e)}")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            return f"document_{timestamp}.html"

    def _get_extension_from_content_type(self, content_type: str) -> str:
        """Determina extensão baseada no content-type"""
        for ext, mime in self.SUPPORTED_MIME_TYPES.items():
            if mime == content_type:
                return ext
        return '.html' if 'html' in content_type else '.txt'

    def _clean_document_url(self, url: str) -> str:
        """Cleans the document URL by removing unnecessary parameters"""
        if not url:
            return url
            
        extensions = list(self.SUPPORTED_MIME_TYPES.keys())
        
        # Find the first occurrence of any supported extension
        for ext in extensions:
            if ext in url.lower():
                # Split at the extension and keep everything before it (inclusive)
                base_url = url.split(ext.lower())[0] + ext.lower()
                return base_url
                
        return url

    async def get_friendly_url(self, doc_id: str, folder_id: Optional[int] = None) -> Optional[str]:
        """Obtém a friendly URL do documento"""
        document_url = f"/o/headless-delivery/v1.0/documents/{doc_id}"
        full_url = f"{self.config.liferay_url}{document_url}"
        
        try:
            async with self.session.get(full_url) as response:
                if response.status == 200:
                    result = await response.json()
                    if result.get("contentUrl"):
                        friendly_url = result["contentUrl"]
                        # Clean the URL before returning
                        cleaned_url = self._clean_document_url(friendly_url)
                        logger.info(f"Friendly URL obtida: {cleaned_url}")
                        return cleaned_url
                logger.error(f"Erro ao obter detalhes do documento: {response.status}")
                
        except Exception as e:
            logger.error(f"Erro ao obter friendly URL: {str(e)}")
            logger.error(traceback.format_exc())
            
        return None

    def _is_valid_file_url(self, url: str) -> bool:
        """Verifica se a URL do arquivo atende aos critérios"""
        if not url:
            return False
            
        if 'sinj' in url.lower():
            return False
            
        url_lower = url.lower()
        valid_patterns = [
            '/wp-content',
            '/wp-conteudo',
            '.df.gov.br/wp-'
        ]
        
        has_valid_pattern = any(pattern.lower() in url_lower for pattern in valid_patterns)
        is_image = any(ext in url_lower for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp'])
        
        return has_valid_pattern or is_image

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitiza o nome do arquivo para evitar problemas no upload"""
        # First decode URL-encoded characters
        filename = unquote(filename)
        
        # Remove trailing spaces
        filename = filename.strip()
        
        # Replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Ensure no trailing spaces in extension
        name_parts = filename.rsplit('.', 1)
        if len(name_parts) > 1:
            filename = f"{name_parts[0].strip()}.{name_parts[1].strip()}"
        
        return filename[:240]  # Limita tamanho do nome

    async def _find_existing_document(self, filename: str, folder_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Busca um documento existente pelo nome do arquivo"""
        try:
            # Clean filename first
            filename = filename.strip()
            
            if folder_id:
                search_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{folder_id}/documents"
            else:
                search_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/documents"
            
            # Check cache first (with clean filename)
            cached_doc = self.cache.get_by_filename(filename)
            if cached_doc:
                return cached_doc
                
            # Try exact match first
            params = {
                'filter': f"title eq '{filename}'",
                'fields': 'id,contentUrl',
                'page': 1,
                'pageSize': 1
            }
            
            async with self.session.get(search_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    items = data.get('items', [])
                    if items:
                        doc = items[0]
                        self.cache.add_filename_mapping(filename, doc)
                        return doc
                        
            # If no exact match, try with URL-encoded space
            encoded_filename = filename.replace(' ', '%20')
            params['filter'] = f"title eq '{encoded_filename}'"
            
            async with self.session.get(search_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    items = data.get('items', [])
                    if items:
                        doc = items[0]
                        self.cache.add_filename_mapping(filename, doc)
                        return doc
            
            # If still no match, try with trailing space
            params['filter'] = f"title eq '{filename} '"
            
            async with self.session.get(search_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    items = data.get('items', [])
                    if items:
                        doc = items[0]
                        self.cache.add_filename_mapping(filename, doc)
                        return doc
            
            return None
            
        except Exception as e:
            logger.error(f"Erro ao buscar documento existente: {str(e)}")
            return None

    async def migrate_document(self, doc_url: str, folder_id: Optional[int] = None, page_url: str = "", hierarchy: str = "") -> Optional[str]:
        """Migra um documento com tratamento de conflitos otimizado"""
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                if not self._is_valid_file_url(doc_url):
                    return None

                # Check cache
                cached_url = self.cache.get_by_url(doc_url)
                if cached_url:
                    return cached_url

                if self.cache.is_processed(doc_url):
                    return None
                
                self.cache.add_url_mapping(doc_url, None)
                logger.info(f"Iniciando migração do documento: {doc_url}")

                # Download do documento
                async with self.session.get(doc_url, allow_redirects=True, timeout=60) as response:
                    if response.status != 200:
                        raise Exception(f"GET request failed with status {response.status}")

                    content = await response.read()
                    if not content:
                        raise Exception("Empty content")

                    filename = self._sanitize_filename(self._extract_filename(doc_url))
                    logger.info(f"Nome do arquivo gerado: {filename}")

                    # Check existing document BEFORE upload
                    existing_doc = await self._find_existing_document(filename, folder_id)
                    if existing_doc:
                        content_url = existing_doc.get('contentUrl')
                        if content_url:
                            self.cache.add_url_mapping(doc_url, content_url)
                            logger.info(f"Documento já existe, reusando URL: {content_url}")
                            return content_url

                    # Prepare upload
                    data = aiohttp.FormData()
                    data.add_field('file', content, 
                                filename=filename,
                                content_type=self._get_mime_type(filename))
                    
                    document_metadata = {
                        "title": filename,
                        "description": f"Migrado de {doc_url}"
                    }
                    
                    data.add_field('documentMetadata',
                                json.dumps(document_metadata),
                                content_type='application/json')

                    # Define upload URL
                    if folder_id:
                        upload_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{folder_id}/documents"
                    else:
                        upload_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/documents"

                    # Try upload
                    async with self.session.post(upload_url, data=data, timeout=120) as upload_response:
                        if upload_response.status in (200, 201):
                            result = await upload_response.json()
                            doc_id = result.get('id')
                            if doc_id:
                                friendly_url = await self.get_friendly_url(doc_id, folder_id)
                                if friendly_url:
                                    self.cache.add_url_mapping(doc_url, friendly_url)
                                    logger.info(f"Documento migrado com sucesso: {friendly_url}")
                                    return friendly_url
                        
                        elif upload_response.status == 409:
                            # On conflict, immediately check for existing document
                            logger.warning(f"Conflito detectado para {filename}, buscando documento existente...")
                            existing_doc = await self._find_existing_document(filename, folder_id)
                            if existing_doc:
                                content_url = existing_doc.get('contentUrl')
                                if content_url:
                                    self.cache.add_url_mapping(doc_url, content_url)
                                    logger.info(f"URL do documento existente recuperada: {content_url}")
                                    return content_url
                            
                            # If still no match, try with other filename variations
                            encoded_filename = filename.replace(' ', '%20')
                            existing_doc = await self._find_existing_document(encoded_filename, folder_id)
                            if existing_doc:
                                content_url = existing_doc.get('contentUrl')
                                if content_url:
                                    self.cache.add_url_mapping(doc_url, content_url)
                                    logger.info(f"URL do documento existente recuperada (encoded): {content_url}")
                                    return content_url
                            
                            response_text = await upload_response.text()
                            raise Exception(f"Conflito não resolvido: {response_text}")
                        
                        else:
                            response_text = await upload_response.text()
                            raise Exception(f"Upload falhou: {upload_response.status} - {response_text}")

                return None

            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"Tentativa {retry_count} falhou para {doc_url}: {str(e)}. Tentando novamente...")
                    await asyncio.sleep(1)
                    continue
                else:
                    logger.error(f"Erro ao migrar documento {doc_url} após {max_retries} tentativas: {str(e)}")
                    self.cache.mark_failed(doc_url)
                    return None
                    
    async def process_page_content(self, page_url: str, folder_id: Optional[int] = None) -> List[str]:
        """Processa uma página inteira de forma otimizada"""
        migrated_urls = []
        
        try:
            if not self.session:
                await self.initialize_session()

            logger.info(f"Processando página: {page_url}")
            
            urls_to_process = await self._collect_urls(page_url)
            total_urls = len(urls_to_process)
            processed = 0
            
            # Processa em lotes menores para melhor controle
            batch_size = 5  # Reduzido para 5 requisições simultâneas
            sem = asyncio.Semaphore(batch_size)
            
            async def process_url_with_sem(url):
                async with sem:
                    try:
                        return await self.migrate_document(url, folder_id, page_url)
                    except Exception as e:
                        logger.error(f"Erro processando {url}: {str(e)}")
                        return None

            for i in range(0, total_urls, batch_size):
                batch = list(urls_to_process)[i:i + batch_size]
                tasks = [process_url_with_sem(url) for url in batch]
                results = await asyncio.gather(*tasks)
                
                for url, result in zip(batch, results):
                    processed += 1
                    if result:
                        migrated_urls.append(result)
                        logger.info(f"Updated document/image URL: {url} -> {result}")
                    else:
                        logger.error(f"Failed to process document/image: {url}")
                    
                # Log do progresso
                logger.info(f"Progresso: {processed}/{total_urls} ({(processed/total_urls*100):.1f}%)")
                
        except Exception as e:
            logger.error(f"Erro ao processar página {page_url}: {str(e)}")
            logger.error(traceback.format_exc())
            
        return migrated_urls

    async def close(self):
        """Fecha recursos"""
        if self.session:
            await self.session.close()
            self.session = None