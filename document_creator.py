import json
import logging
import traceback
from typing import Optional, Dict, Any
import aiohttp
from urllib.parse import urlparse, unquote
from datetime import datetime

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
    }

    def __init__(self, config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.error_log_file = "migration_errors.txt"
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
            }
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

    async def migrate_document(self, doc_url: str, folder_id: Optional[int] = None, page_url: str = "", hierarchy: str = "") -> Optional[str]:
        try:
            if not self.session:
                await self.initialize_session()

            logger.info(f"Iniciando migração do documento: {doc_url}")
            
            parsed_url = urlparse(doc_url)
            filename = self._sanitize_filename(parsed_url.path.split('/')[-1])
            
            async with self.session.head(doc_url) as head_response:
                content_length = int(head_response.headers.get('Content-Length', '0'))
                
                if content_length > 104857600:  # Limite de 100MB
                    error_msg = f"Documento muito grande para migração: {content_length} bytes"
                    logger.warning(error_msg)
                    self._log_migration_error(doc_url, error_msg, page_url, hierarchy)
                    return None

            async with self.session.get(doc_url) as response:
                if response.status != 200:
                    error_msg = f"Falha ao baixar documento: {response.status}"
                    logger.error(error_msg)
                    self._log_migration_error(doc_url, error_msg, page_url, hierarchy)
                    return None

                content = await response.read()
                content_type = response.headers.get('content-type', '') or self._get_mime_type(filename)

                file_size = len(content)
                logger.info(f"Tamanho do documento baixado: {file_size} bytes")

                if file_size == 0:
                    error_msg = "O arquivo baixado está vazio"
                    logger.error(error_msg)
                    self._log_migration_error(doc_url, error_msg, page_url, hierarchy)
                    return None

                document_metadata = {
                    "title": filename,
                    "description": f"Migrado de {doc_url}"
                }

                data = aiohttp.FormData()
                data.add_field(
                    'file',
                    content,
                    filename=filename,
                    content_type=content_type
                )
                data.add_field(
                    'documentMetadata',
                    json.dumps(document_metadata),
                    content_type='application/json'
                )
                
                if folder_id is None or folder_id == 0:
                    upload_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/documents"
                else:
                    upload_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{folder_id}/documents"
                
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

                    error_msg = f"Falha no upload: {upload_response.status}\nResposta do servidor: {response_text}"
                    logger.error(f"Falha no upload: {upload_response.status}")
                    logger.error(f"Resposta do servidor: {response_text}")
                    self._log_migration_error(doc_url, error_msg, page_url, hierarchy)
                    return None
                
        except Exception as e:
            error_msg = f"Erro inesperado ao migrar documento: {str(e)}\n{traceback.format_exc()}"
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