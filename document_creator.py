import json
import logging
import traceback
from typing import List, Optional, Dict, Any, Set
import aiohttp
from urllib.parse import urljoin, urlparse, unquote
from datetime import datetime

from bs4 import BeautifulSoup

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
        '.htm': 'text/html'
    }

    def __init__(self, config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.error_log_file = "migration_errors.txt"
        self.processed_urls: Set[str] = set()
        self._initialize_logging()

    def _initialize_logging(self):
        """Configure detailed logging for the document migration process"""
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    def _log_migration_error(self, doc_url: str, error_msg: str, page_url: str, hierarchy: str):
        """
        Registra erros de migração em um arquivo de texto
        """
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
        """Initialize HTTP session with robust authentication"""
        if self.session:
            await self.session.close()
        
        auth = aiohttp.BasicAuth(
            login=self.config.liferay_user,
            password=self.config.liferay_pass
        )
        
        timeout = aiohttp.ClientTimeout(total=300)  # 5 minutos de timeout
        
        self.session = aiohttp.ClientSession(
            auth=auth,
            timeout=timeout,
            headers={
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            },
            connector=aiohttp.TCPConnector(ssl=False)
        )
        
        try:
            async with self.session.get(f"{self.config.liferay_url}/c/portal/login") as response:
                if response.status != 200:
                    logger.error(f"Falha na autenticação inicial: {response.status}")
        except Exception as e:
            logger.error(f"Erro ao iniciar autenticação: {str(e)}")

    def _get_mime_type(self, filename: str) -> str:
        """Determina o MIME type pelo nome do arquivo"""
        ext = '.' + filename.split('.')[-1].lower() if '.' in filename else ''
        return self.SUPPORTED_MIME_TYPES.get(ext, 'application/octet-stream')

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitiza o nome do arquivo para evitar problemas no upload"""
        filename = unquote(filename)
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename

    def _extract_filename(self, url: str, content_type: str = '') -> str:
        """
        Extrai um nome de arquivo válido da URL ou gera um baseado no content-type
        """
        try:
            # Tenta extrair o nome do arquivo da URL
            parsed_url = urlparse(url)
            path = unquote(parsed_url.path)
            
            # Se o path termina com /, remove
            if path.endswith('/'):
                path = path[:-1]
                
            # Pega a última parte do path
            filename = path.split('/')[-1]
            
            # Se não conseguiu extrair um nome válido
            if not filename or filename.startswith('.') or '.' not in filename:
                # Gera um nome baseado no hostname e timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                hostname = parsed_url.hostname.split('.')[0] if parsed_url.hostname else 'download'
                
                # Define a extensão baseada no content-type
                ext = ''
                if content_type:
                    for file_ext, mime_type in self.SUPPORTED_MIME_TYPES.items():
                        if mime_type == content_type:
                            ext = file_ext
                            break
                    if not ext:
                        ext = '.html' if 'html' in content_type else '.txt'
                else:
                    ext = '.html'  # default extension
                
                filename = f"{hostname}_{timestamp}{ext}"
            
            # Sanitiza o nome do arquivo
            return self._sanitize_filename(filename)
            
        except Exception as e:
            logger.error(f"Erro ao extrair nome do arquivo: {str(e)}")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            return f"document_{timestamp}.html"

    def _validate_url(self, url: str) -> bool:
        """Valida se a URL está em um formato correto"""
        try:
            parsed = urlparse(url)
            return all([parsed.scheme, parsed.netloc]) and parsed.scheme in ['http', 'https']
        except Exception:
            return False

    # DOCUMENT VALIDATE 
    async def process_page_content(self, page_url: str, folder_id: Optional[int] = None) -> List[str]:
        """
        Processa uma página, busca arquivos e imagens dentro da classe .paginas-internas
        """
        migrated_urls = []
        
        try:
            if not self.session:
                await self.initialize_session()

            logger.info(f"Processando página: {page_url}")
            
            async with self.session.get(page_url) as response:
                if response.status != 200:
                    logger.error(f"Falha ao acessar página {page_url}: {response.status}")
                    return migrated_urls
                
                html_content = await response.text()
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Procura o elemento com classe 'paginas-internas'
                content_div = soup.find(class_='paginas-internas')
                if not content_div:
                    logger.warning(f"Classe 'paginas-internas' não encontrada em {page_url}")
                    return migrated_urls
                
                # Processa todos os links
                all_links = content_div.find_all('a')
                for link in all_links:
                    href = link.get('href')
                    if href:
                        await self._process_url(href, page_url, folder_id, migrated_urls)
                
                # Processa todas as imagens
                all_images = content_div.find_all('img')
                for img in all_images:
                    # Processa o src principal
                    src = img.get('src')
                    if src:
                        await self._process_url(src, page_url, folder_id, migrated_urls)
                    
                    # Processa o srcset se existir
                    srcset = img.get('srcset')
                    if srcset:
                        # Divide o srcset em URLs individuais
                        srcset_urls = [url.strip().split(' ')[0] for url in srcset.split(',')]
                        for url in srcset_urls:
                            await self._process_url(url, page_url, folder_id, migrated_urls)
                            
        except Exception as e:
            logger.error(f"Erro ao processar página {page_url}: {str(e)}")
            logger.error(traceback.format_exc())
            
        return migrated_urls

    async def _process_url(self, url: str, page_url: str, folder_id: Optional[int], migrated_urls: List[str]):
        """
        Processa uma URL individual, seja de link ou imagem
        """
        # Converte URL relativa para absoluta
        absolute_url = urljoin(page_url, url)
        
        # Verifica se a URL já foi processada
        if absolute_url in self.processed_urls:
            return
        
        # Verifica se a URL atende aos critérios
        if self._is_valid_file_url(absolute_url):
            logger.info(f"Encontrado arquivo válido: {absolute_url}")
            
            # Tenta migrar o documento
            friendly_url = await self.migrate_document(
                doc_url=absolute_url,
                folder_id=folder_id,
                page_url=page_url,
                hierarchy="paginas-internas"
            )
            
            if friendly_url:
                migrated_urls.append(friendly_url)
                self.processed_urls.add(absolute_url)
                logger.info(f"✓ Arquivo migrado com sucesso: {friendly_url}")
            else:
                logger.error(f"✗ Falha ao migrar arquivo: {absolute_url}")

    def _is_valid_file_url(self, url: str) -> bool:
        """Verifica se a URL do arquivo atende aos critérios especificados"""
        if not url:
            return False
            
        # Ignora URLs que contêm 'sinj'
        if 'sinj' in url.lower():
            return False
            
        # Converte a URL para minúsculas para fazer a comparação
        url_lower = url.lower()
        
        # Verifica se a URL contém algum dos padrões permitidos
        valid_patterns = [
            '/wp-content',
            '/wp-conteudo',
            '.df.gov.br/wp-'
        ]
        
        # Verifica os padrões na URL
        has_valid_pattern = any(pattern.lower() in url_lower for pattern in valid_patterns)
        
        # Se for uma imagem, também verifica extensões comuns de imagem
        is_image = any(ext in url_lower for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp'])
        
        return has_valid_pattern


    async def migrate_document(self, doc_url: str, folder_id: Optional[int] = None, page_url: str = "", hierarchy: str = "") -> Optional[str]:
        try:
            if not self.session:
                await self.initialize_session()

            # Valida a URL antes de prosseguir
            if not self._validate_url(doc_url):
                error_msg = f"URL inválida: {doc_url}"
                logger.error(error_msg)
                self._log_migration_error(doc_url, error_msg, page_url, hierarchy)
                return None

            logger.info(f"Iniciando migração do documento: {doc_url}")
            
            # Primeiro faz um HEAD request para obter informações
            async with self.session.head(doc_url, allow_redirects=True) as head_response:
                if head_response.status != 200:
                    error_msg = f"Falha ao verificar documento: {head_response.status}"
                    logger.error(error_msg)
                    self._log_migration_error(doc_url, error_msg, page_url, hierarchy)
                    return None
                
                content_length = int(head_response.headers.get('Content-Length', '0'))
                content_type = head_response.headers.get('content-type', '').split(';')[0]
                
                if content_length > 104857600:  # 100MB limit
                    error_msg = f"Documento muito grande: {content_length} bytes"
                    logger.warning(error_msg)
                    self._log_migration_error(doc_url, error_msg, page_url, hierarchy)
                    return None

            # Faz o download do conteúdo
            async with self.session.get(doc_url, allow_redirects=True) as response:
                if response.status != 200:
                    error_msg = f"Falha ao baixar documento: {response.status}"
                    logger.error(error_msg)
                    self._log_migration_error(doc_url, error_msg, page_url, hierarchy)
                    return None

                content = await response.read()
                content_type = response.headers.get('content-type', '').split(';')[0]
                
                file_size = len(content)
                if file_size == 0:
                    error_msg = "Arquivo vazio"
                    logger.error(error_msg)
                    self._log_migration_error(doc_url, error_msg, page_url, hierarchy)
                    return None

                # Extrai e valida o nome do arquivo
                filename = self._extract_filename(doc_url, content_type)
                logger.info(f"Nome do arquivo gerado: {filename}")

                # Prepara os metadados
                document_metadata = {
                    "title": filename,
                    "description": f"Migrado de {doc_url}"
                }

                # Prepara o upload
                data = aiohttp.FormData()
                data.add_field(
                    'file',
                    content,
                    filename=filename,
                    content_type=content_type or self._get_mime_type(filename)
                )
                data.add_field(
                    'documentMetadata',
                    json.dumps(document_metadata),
                    content_type='application/json'
                )
                
                # Define a URL de upload
                if folder_id is None or folder_id == 0:
                    upload_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/documents"
                else:
                    upload_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{folder_id}/documents"
                
                # Faz o upload
                async with self.session.post(upload_url, data=data) as upload_response:
                    response_text = await upload_response.text()

                    if upload_response.status in (200, 201):
                        result = await upload_response.json()
                        doc_id = result.get('id')

                        if doc_id:
                            friendly_url = await self.get_friendly_url(doc_id, folder_id)
                            if friendly_url:
                                logger.info(f"Documento migrado com sucesso para: {friendly_url}")
                                return friendly_url

                    error_msg = f"Falha no upload: {upload_response.status}\nResposta: {response_text}"
                    logger.error(f"Falha no upload: {upload_response.status}")
                    logger.error(f"Resposta do servidor: {response_text}")
                    self._log_migration_error(doc_url, error_msg, page_url, hierarchy)
                    return None
                
        except Exception as e:
            error_msg = f"Erro inesperado: {str(e)}\n{traceback.format_exc()}"
            logger.error(f"Erro inesperado ao migrar documento {doc_url}: {str(e)}")
            logger.error(traceback.format_exc())
            self._log_migration_error(doc_url, error_msg, page_url, hierarchy)
        
        return None

    async def get_friendly_url(self, doc_id, folder_id):
        """
        Obtém a friendly URL do documento usando a estrutura correta do Liferay
        """
        document_url = f"/o/headless-delivery/v1.0/documents/{doc_id}"
        full_url = f"{self.config.liferay_url}{document_url}"
        
        try:
            async with self.session.get(full_url) as response:
                if response.status == 200:
                    result = await response.json()
                    
                    if result.get("contentUrl"):
                        friendly_url = result["contentUrl"]
                        logger.info(f"Friendly URL obtida: {friendly_url}")
                        return friendly_url
                        
                    logger.error("contentUrl não encontrada no documento")
                    return None
                
                logger.error(f"Erro ao obter detalhes do documento: {response.status}")
                return None
                
        except Exception as e:
            logger.error(f"Erro ao obter friendly URL: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None
            