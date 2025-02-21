import json
import logging
import traceback
from typing import List, Optional, Dict, Any, Set, Tuple
import aiohttp
from urllib.parse import urljoin, urlparse, unquote
from datetime import datetime
from bs4 import BeautifulSoup, SoupStrainer
import asyncio
from dataclasses import dataclass
from functools import lru_cache
from cache.document_cache import DocumentCache
import os
from concurrent.futures import ThreadPoolExecutor

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
        # Aumentado para maior paralelismo
        self.semaphore = asyncio.Semaphore(40)  
        self.batch_size = 50
        self.error_log_file = "migration_errors.log"
        # ThreadPool para operações CPU-bound
        cpu_count = os.cpu_count() or 4
        self.thread_pool = ThreadPoolExecutor(max_workers=min(32, cpu_count * 4))
        # Cache em memória para operações frequentes
        self._url_cache = {}
        self._filename_cache = {}
        self._dns_cache = {}
        # Conexões e pooling
        self._connection_pool = None
        self._initialize_logging()
        # Strainer para parsing seletivo de BeautifulSoup
        self._content_strainer = SoupStrainer(['a', 'img'])
        # Contadores para estatísticas
        self._stats = {
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "cached": 0,
            "retries": 0
        }

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
        """Initialize HTTP session com otimizações"""
        if self.session:
            await self.session.close()
        
        # Otimiza pool de conexões
        if not self._connection_pool:
            self._connection_pool = aiohttp.TCPConnector(
                ssl=False,
                limit=150,  # Triplicado para maior paralelismo
                ttl_dns_cache=600,  # Cache de DNS por 10 minutos
                keepalive_timeout=120,  # Keep-alive mais longo
                force_close=False  # Permite reuso de conexões
            )
        
        auth = aiohttp.BasicAuth(
            login=self.config.liferay_user,
            password=self.config.liferay_pass
        )
        
        # Timeout aumentado para downloads maiores
        timeout = aiohttp.ClientTimeout(total=180, connect=30, sock_read=120)
        
        self.session = aiohttp.ClientSession(
            auth=auth,
            timeout=timeout,
            headers={
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Connection': 'keep-alive'  # Força conexões persistentes
            },
            connector=self._connection_pool,
            json_serialize=json.dumps  # Serialização JSON mais rápida
        )
        logger.info("Session initialized with high-performance settings")

    async def _process_in_thread(self, func, *args, **kwargs):
        """Executa função CPU-bound em thread separada"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool, 
            lambda: func(*args, **kwargs)
        )

    @lru_cache(maxsize=500)
    def _get_mime_type(self, filename: str) -> str:
        """Determina o MIME type pelo nome do arquivo"""
        ext = '.' + filename.split('.')[-1].lower() if '.' in filename else ''
        return self.SUPPORTED_MIME_TYPES.get(ext, 'application/octet-stream')

    async def _collect_urls(self, page_url: str) -> Set[str]:
        """Coleta URLs com parsing otimizado"""
        urls_to_process = set()
        
        try:
            async with self.session.get(page_url) as response:
                if response.status != 200:
                    logger.warning(f"Falha ao acessar a página {page_url}: status {response.status}")
                    return urls_to_process
                    
                html_content = await response.text()
                
                # Parse seletivo apenas dos elementos que nos interessam
                soup = await self._process_in_thread(
                    BeautifulSoup, 
                    html_content, 
                    'html.parser', 
                    parse_only=self._content_strainer
                )
                
                content_div = soup.find(class_='paginas-internas')
                
                if not content_div:
                    logger.warning(f"Elemento 'paginas-internas' não encontrado em {page_url}")
                    return urls_to_process

                # Buscas otimizadas em uma única passagem
                tasks = [
                    self._extract_links(content_div, page_url),
                    self._extract_images(content_div, page_url),
                    self._extract_srcset(content_div, page_url)
                ]
                
                # Processa tudo em paralelo
                results = await asyncio.gather(*tasks)
                
                # Combina resultados
                for result_set in results:
                    urls_to_process.update(result_set)
                
                logger.info(f"Coletadas {len(urls_to_process)} URLs para processamento em {page_url}")
                
        except Exception as e:
            logger.error(f"Erro ao coletar URLs em {page_url}: {str(e)}")
            
        return urls_to_process

    async def _extract_links(self, content_div, page_url) -> Set[str]:
        """Extrai links de forma paralela"""
        urls = set()
        for a in content_div.find_all('a', href=True):
            url = a.get('href')
            if url:
                absolute_url = urljoin(page_url, url)
                if self._is_valid_file_url(absolute_url):
                    urls.add(absolute_url)
        return urls
        
    async def _extract_images(self, content_div, page_url) -> Set[str]:
        """Extrai imagens de forma paralela"""
        urls = set()
        for img in content_div.find_all('img', src=True):
            url = img.get('src')
            if url:
                absolute_url = urljoin(page_url, url)
                if self._is_valid_file_url(absolute_url):
                    urls.add(absolute_url)
        return urls
        
    async def _extract_srcset(self, content_div, page_url) -> Set[str]:
        """Extrai srcset de forma paralela"""
        urls = set()
        for img in content_div.find_all('img', srcset=True):
            srcset = img.get('srcset')
            if srcset:
                for url in [u.strip().split(' ')[0] for u in srcset.split(',')]:
                    absolute_url = urljoin(page_url, url)
                    if self._is_valid_file_url(absolute_url):
                        urls.add(absolute_url)
        return urls

    async def _process_url_batch(self, urls: List[str], folder_id: Optional[int], page_url: str) -> List[str]:
        """Processa lotes de URLs com controle de concorrência otimizado"""
        # Verifica cache em paralelo primeiro
        filtered_urls = []
        cached_results = []
        
        for url in urls:
            # Checa cache primeiro
            cached_url = self.cache.get_by_url(url)
            if cached_url:
                self._stats["cached"] += 1
                cached_results.append(cached_url)
                continue
                
            if not self.cache.is_processed(url) and not self.cache.is_failed(url):
                filtered_urls.append(url)
        
        if not filtered_urls:
            return cached_results
            
        logger.info(f"Processando lote de {len(filtered_urls)} URLs (ignoradas {len(urls) - len(filtered_urls)} já processadas)")
        
        # Cria tarefas com semáforo incorporado
        semaphore = asyncio.Semaphore(40)  # Limita execução paralela
        
        async def process_with_semaphore(url):
            async with semaphore:
                return await self._process_single_url(url, folder_id, page_url)
        
        tasks = [process_with_semaphore(url) for url in filtered_urls]
        
        # Executa em paralelo com controle de concorrência
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Processa resultados
        successful_urls = cached_results.copy()  # Inclui URLs do cache
        for i, result in enumerate(results):
            if isinstance(result, str) and result:
                successful_urls.append(result)
                self._stats["successful"] += 1
            elif isinstance(result, Exception):
                logger.error(f"Erro ao processar {filtered_urls[i]}: {str(result)}")
                self._stats["failed"] += 1
                
        self._stats["processed"] += len(filtered_urls)
        
        # Reporta progresso
        success_rate = (len(successful_urls) / len(urls)) * 100 if urls else 0
        logger.info(f"Lote concluído: {len(successful_urls)}/{len(urls)} URLs processadas com sucesso ({success_rate:.1f}%)")
        
        return successful_urls

    async def _process_single_url(self, url: str, folder_id: Optional[int], page_url: str) -> Optional[str]:
        """Processa URL individual com retry em caso de falha"""
        try:
            friendly_url = await self.migrate_document(url, folder_id, page_url)
            if friendly_url:
                logger.info(f"URL processada com sucesso: {url} -> {friendly_url}")
            return friendly_url
        except Exception as e:
            logger.error(f"Erro ao processar URL {url}: {str(e)}")
            self._log_migration_error(url, str(e), page_url, "")
            return None

    def _extract_filename(self, url: str, content_type: str = '') -> str:
        """Extrai e normaliza o nome do arquivo"""
        try:
            parsed_url = urlparse(url)
            path = unquote(parsed_url.path)
            filename = path.rstrip('/').split('/')[-1]
            
            # Se não tem extensão, usa o content-type
            if not filename or '.' not in filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                hostname = parsed_url.hostname.split('.')[0] if parsed_url.hostname else 'download'
                ext = self._get_extension_from_content_type(content_type)
                filename = f"{hostname}_{timestamp}{ext}"
                return self._sanitize_filename(filename)

            # Normaliza a extensão para minúscula
            name, ext = filename.rsplit('.', 1)
            ext = ext.lower()
            
            # Mapeia extensões para garantir consistência
            ext_mapping = {
                'docx': 'doc',
                'xlsx': 'xls',
                'pptx': 'ppt'
            }
            
            ext = ext_mapping.get(ext, ext)
            filename = f"{name}.{ext}"
            
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
        """Limpa a URL do documento removendo parâmetros desnecessários"""
        if not url:
            return url
            
        # Remove parâmetros de versão e download
        url_parts = url.split('?')[0]
        url_parts = url_parts.split('/version/')[0]
        
        # Remove parâmetros após o nome do arquivo
        for ext in self.SUPPORTED_MIME_TYPES.keys():
            if ext in url_parts.lower():
                base_parts = url_parts.split(ext.lower())
                if len(base_parts) > 1:
                    clean_url = base_parts[0] + ext.lower()
                    return clean_url
                    
        return url_parts

    async def get_friendly_url(self, doc_id: str, folder_id: Optional[int] = None) -> Optional[str]:
        """Obtém a friendly URL do documento de forma otimizada"""
        document_url = f"/o/headless-delivery/v1.0/documents/{doc_id}"
        full_url = f"{self.config.liferay_url}{document_url}"
        
        try:
            async with self.session.get(full_url) as response:
                if response.status == 200:
                    result = await response.json()
                    if result.get("contentUrl"):
                        friendly_url = result["contentUrl"]
                        # Clean up the URL before returning
                        cleaned_url = self._clean_document_url(friendly_url)
                        return cleaned_url
                logger.error(f"Erro ao obter detalhes do documento: {response.status}")
                
        except Exception as e:
            logger.error(f"Erro ao obter friendly URL: {str(e)}")
            
        return None

    def _is_valid_file_url(self, url: str) -> bool:
        """Verifies if file URL meets criteria"""
        if not url:
            return False
            
        # Clean up URLs with spaces (often caused by HTML parsing issues)
        if ' http' in url:
            url = url.split(' http')[1]
            if not url.startswith('http'):
                url = 'http' + url
                
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
        """Busca documento existente com melhor cache e processamento paralelo"""
        try:
            # Limpa e normaliza o nome do arquivo
            filename = self._sanitize_filename(filename)
            name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
            
            # Verifica todas as variações possíveis do nome
            possible_names = [
                filename,  # original
                f"{name}.docx" if ext == 'doc' else filename,  # tenta .docx se for .doc
                f"{name}.xlsx" if ext == 'xls' else filename,  # tenta .xlsx se for .xls
                f"{name}.pptx" if ext == 'ppt' else filename,  # tenta .pptx se for .ppt
            ]
            
            # Verifica cache primeiro para todas as variações
            for name_variant in possible_names:
                cached_doc = self.cache.get_by_filename(name_variant)
                if cached_doc:
                    logger.info(f"Documento encontrado no cache: {name_variant}")
                    return cached_doc
            
            # Se não encontrou no cache, busca em paralelo no Liferay
            search_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{folder_id}/documents" if folder_id else f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/documents"

            tasks = []
            for name_variant in possible_names:
                params = {
                    'filter': f"title eq '{name_variant}'",
                    'fields': 'id,contentUrl',
                    'page': 1,
                    'pageSize': 1
                }
                tasks.append(self.session.get(search_url, params=params))
            
            # Executa todas as buscas em paralelo
            responses = await asyncio.gather(*tasks)
            
            for i, response in enumerate(responses):
                if response.status == 200:
                    data = await response.json()
                    items = data.get('items', [])
                    if items:
                        doc = items[0]
                        logger.info(f"Documento existente encontrado: {possible_names[i]}")
                        # Adiciona no cache todas as variações do nome
                        for variant in possible_names:
                            self.cache.add_filename_mapping(variant, doc)
                        return doc
            
            logger.info(f"Documento não encontrado: {filename}")
            return None
            
        except Exception as e:
            logger.error(f"Erro ao buscar documento existente: {str(e)}")
            return None
    
    async def migrate_document(self, doc_url: str, folder_id: Optional[int] = None, page_url: str = "", hierarchy: str = "") -> Optional[str]:
        """Migra um documento com retry exponencial e paralelismo otimizado"""
        max_retries = 4  # Aumentado para mais tentativas
        retry_count = 0
        backoff_time = 1

        while retry_count < max_retries:
            try:
                if not self._is_valid_file_url(doc_url):
                    logger.info(f"URL inválida, ignorando: {doc_url}")
                    return None

                # Check cache
                cached_url = self.cache.get_by_url(doc_url)
                if cached_url:
                    # Clean up cached URL before returning
                    cleaned_cached_url = self._clean_document_url(cached_url)
                    logger.info(f"URL encontrada no cache e limpa: {doc_url} -> {cleaned_cached_url}")
                    return cleaned_cached_url

                if self.cache.is_processed(doc_url):
                    logger.info(f"URL já processada anteriormente: {doc_url}")
                    return None
                
                self.cache.add_url_mapping(doc_url, None)
                logger.info(f"Iniciando migração do documento: {doc_url}")

                # Download do documento com timeout aumentado
                download_timeout = aiohttp.ClientTimeout(total=120, sock_read=90)
                async with self.session.get(
                    doc_url, 
                    allow_redirects=True, 
                    timeout=download_timeout,
                    headers={'Accept-Encoding': 'gzip, deflate'}  # Compressão para downloads mais rápidos
                ) as response:
                    if response.status != 200:
                        raise Exception(f"GET request failed with status {response.status}")

                    content = await response.read()
                    if not content:
                        raise Exception("Empty content")

                    # Extração e sanitização do nome em paralelo para não bloquear
                    filename = await self._process_in_thread(
                        lambda: self._sanitize_filename(self._extract_filename(doc_url))
                    )
                    logger.info(f"Nome do arquivo gerado: {filename}")

                    # Busca documento existente e faz upload em paralelo 
                    # para otimizar tempo de resposta
                    existing_doc_task = asyncio.create_task(
                        self._find_existing_document(filename, folder_id)
                    )
                    
                    # Prepara dados para upload
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
                    
                    # Aguarda resultado da busca por documento existente
                    existing_doc = await existing_doc_task
                    if existing_doc:
                        content_url = existing_doc.get('contentUrl')
                        if content_url:
                            # Clean up the content URL before caching and returning
                            cleaned_url = self._clean_document_url(content_url)
                            self.cache.add_url_mapping(doc_url, cleaned_url)
                            logger.info(f"Documento já existe, reusando URL limpa: {cleaned_url}")
                            return cleaned_url

                    # Try to create at site level first if no folder_id
                    upload_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/documents"
                    if folder_id:
                        upload_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{folder_id}/documents"

                    # Timeout específico para upload
                    upload_timeout = aiohttp.ClientTimeout(total=180)
                    
                    # Tenta upload com exponential backoff em caso de falha
                    async with self.session.post(
                        upload_url, 
                        data=data, 
                        timeout=upload_timeout,
                        headers={'Connection': 'keep-alive'}
                    ) as upload_response:
                        if upload_response.status in (200, 201):
                            result = await upload_response.json()
                            doc_id = result.get('id')
                            if doc_id:
                                friendly_url = await self.get_friendly_url(doc_id, folder_id)
                                if friendly_url:
                                    # Clean up friendly URL before caching and returning
                                    cleaned_friendly_url = self._clean_document_url(friendly_url)
                                    self.cache.add_url_mapping(doc_url, cleaned_friendly_url)
                                    logger.info(f"Documento migrado com sucesso: {cleaned_friendly_url}")
                                    return cleaned_friendly_url
                        
                        elif upload_response.status == 409:
                            # Try site-level creation on conflict
                            if folder_id:
                                logger.warning(f"Conflito detectado para {filename}, tentando criar no nível do site...")
                                site_upload_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/documents"
                                
                                async with self.session.post(site_upload_url, data=data, timeout=upload_timeout) as site_response:
                                    if site_response.status in (200, 201):
                                        result = await site_response.json()
                                        doc_id = result.get('id')
                                        if doc_id:
                                            friendly_url = await self.get_friendly_url(doc_id)
                                            if friendly_url:
                                                cleaned_friendly_url = self._clean_document_url(friendly_url)
                                                self.cache.add_url_mapping(doc_url, cleaned_friendly_url)
                                                logger.info(f"Documento migrado com sucesso no nível do site: {cleaned_friendly_url}")
                                                return cleaned_friendly_url
                        
                        response_text = await upload_response.text()
                        raise Exception(f"Upload falhou: {upload_response.status} - {response_text}")

                return None

            except Exception as e:
                retry_count += 1
                # Incrementa contador de retries
                self._stats["retries"] += 1
                
                if retry_count < max_retries:
                    # Backoff exponencial limitado a 30s
                    backoff_time = min(2 ** retry_count, 30)
                    logger.warning(f"Tentativa {retry_count} falhou para {doc_url}: {str(e)}. Tentando novamente em {backoff_time}s...")
                    await asyncio.sleep(backoff_time)
                    continue
                else:
                    logger.error(f"Erro ao migrar documento {doc_url} após {max_retries} tentativas: {str(e)}")
                    self.cache.mark_failed(doc_url)
                    self._log_migration_error(doc_url, str(e), page_url, hierarchy)
                    return None
                    
    async def process_page_content(self, page_url: str, folder_id: Optional[int] = None) -> List[str]:
        """Processa uma página inteira de forma otimizada com processamento paralelo e batch adaptativo"""
        migrated_urls = []
        
        try:
            if not self.session:
                await self.initialize_session()

            logger.info(f"Processando página: {page_url}")
            start_time = datetime.now()
            
            # Coleta URLs
            urls_to_process = await self._collect_urls(page_url)
            total_urls = len(urls_to_process)
            
            if total_urls == 0:
                logger.info(f"Nenhuma URL encontrada para processar em {page_url}")
                return migrated_urls
                
            logger.info(f"Encontradas {total_urls} URLs para processar em {page_url}")
            
            # Convert to list for better batch processing
            url_list = list(urls_to_process)
            processed = 0
            
            # Adapta tamanho do batch baseado no número total
            # Mais URLs = batches maiores, menos URLs = batches menores
            batch_size = min(max(10, total_urls // 10), 30)  # Entre 10 e 30
            
            # Process em batches com relatório de progresso
            for i in range(0, total_urls, batch_size):
                batch = url_list[i:i + batch_size]
                batch_num = i//batch_size + 1
                total_batches = (total_urls + batch_size - 1)//batch_size
                
                logger.info(f"Processando lote {batch_num}/{total_batches} - {len(batch)} URLs")
                batch_start = datetime.now()
                
                # Processa batch
                batch_results = await self._process_url_batch(batch, folder_id, page_url)
                migrated_urls.extend([r for r in batch_results if r])
                
                # Calcula progresso e estatísticas
                processed += len(batch)
                progress_pct = (processed / total_urls) * 100
                batch_time = (datetime.now() - batch_start).total_seconds()
                
                # Estima tempo restante
                elapsed = (datetime.now() - start_time).total_seconds()
                items_per_second = processed / elapsed if elapsed > 0 else 0
                remaining_items = total_urls - processed
                remaining_time = remaining_items / items_per_second if items_per_second > 0 else 0
                
                logger.info(f"Progresso: {progress_pct:.1f}% ({processed}/{total_urls}) - "
                           f"Batch: {len(batch_results)}/{len(batch)} em {batch_time:.1f}s - "
                           f"Taxa: {items_per_second:.2f} items/s - "
                           f"Tempo restante estimado: {remaining_time:.1f}s")
                
                # Pequena pausa entre batches para liberar recursos
                await asyncio.sleep(0.5)
            
            # Estatísticas finais
            total_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Processamento concluído em {total_time:.1f}s - "
                       f"Migrados: {len(migrated_urls)}/{total_urls} documentos "
                       f"({len(migrated_urls)/total_urls*100:.1f}% de sucesso)")
            
        except Exception as e:
            logger.error(f"Erro ao processar página {page_url}: {str(e)}")
            logger.error(traceback.format_exc())
            
        return migrated_urls

    async def process_multiple_pages(self, pages: List[Tuple[str, Optional[int]]], parallel_pages: int = 5) -> Dict[str, List[str]]:
        """
        Processa múltiplas páginas em paralelo
        
        Args:
            pages: Lista de tuplas (page_url, folder_id)
            parallel_pages: Número de páginas para processar simultaneamente
        
        Returns:
            Dicionário mapeando URLs de páginas para lista de URLs migradas
        """
        results = {}
        semaphore = asyncio.Semaphore(parallel_pages)
        
        async def process_with_semaphore(page_url, folder_id):
            async with semaphore:
                return page_url, await self.process_page_content(page_url, folder_id)
        
        tasks = [process_with_semaphore(url, folder_id) for url, folder_id in pages]
        
        logger.info(f"Iniciando processamento de {len(pages)} páginas (máximo {parallel_pages} em paralelo)")
        start_time = datetime.now()
        
        # Processa em lotes para não sobrecarregar
        for i in range(0, len(tasks), parallel_pages):
            batch = tasks[i:i + parallel_pages]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Erro no processamento de página: {str(result)}")
                    continue
                    
                page_url, migrated = result
                results[page_url] = migrated
                
            # Reporta progresso
            completed = min(i + parallel_pages, len(tasks))
            logger.info(f"Progresso: {completed}/{len(tasks)} páginas processadas")
        
        total_time = (datetime.now() - start_time).total_seconds()
        total_migrated = sum(len(urls) for urls in results.values())
        
        logger.info(f"Processamento multi-página concluído em {total_time:.1f}s")
        logger.info(f"Total de documentos migrados: {total_migrated}")
        logger.info(f"Estatísticas: {self._stats}")
        
        return results

    async def batch_migrate_documents(self, doc_urls: List[str], folder_id: Optional[int] = None, 
                                   batch_size: int = 20) -> List[str]:
        """
        Migra uma lista de documentos em batches de forma otimizada
        
        Args:
            doc_urls: Lista de URLs de documentos para migrar
            folder_id: ID da pasta destino (opcional)
            batch_size: Tamanho do batch
            
        Returns:
            Lista de URLs migradas
        """
        migrated_urls = []
        total = len(doc_urls)
        
        if not total:
            return migrated_urls
            
        if not self.session:
            await self.initialize_session()
        
        logger.info(f"Iniciando migração em lote de {total} documentos (batch_size={batch_size})")
        start_time = datetime.now()
        
        # Processa em batches
        for i in range(0, total, batch_size):
            batch = doc_urls[i:i + batch_size]
            logger.info(f"Processando batch {i//batch_size + 1}/{(total+batch_size-1)//batch_size} - {len(batch)} URLs")
            
            batch_start = datetime.now()
            
            # Executa migrações em paralelo com semáforo
            tasks = []
            semaphore = asyncio.Semaphore(batch_size)
            
            async def migrate_with_semaphore(url):
                async with semaphore:
                    return await self.migrate_document(url, folder_id)
            
            tasks = [migrate_with_semaphore(url) for url in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Processa resultados
            batch_migrated = []
            for j, result in enumerate(results):
                if isinstance(result, str) and result:
                    batch_migrated.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Erro ao migrar {batch[j]}: {str(result)}")
            
            migrated_urls.extend(batch_migrated)
            
            # Estatísticas do batch
            batch_time = (datetime.now() - batch_start).total_seconds()
            processed = i + len(batch)
            progress_pct = (processed / total) * 100
            
            # Estima tempo restante
            elapsed = (datetime.now() - start_time).total_seconds()
            items_per_second = processed / elapsed if elapsed > 0 else 0
            remaining_items = total - processed
            remaining_time = remaining_items / items_per_second if items_per_second > 0 else 0
            
            logger.info(f"Batch concluído: {len(batch_migrated)}/{len(batch)} migrados ({len(batch_migrated)/len(batch)*100:.1f}%)")
            logger.info(f"Progresso: {progress_pct:.1f}% ({processed}/{total}) - "
                       f"Taxa: {items_per_second:.2f} items/s - "
                       f"Tempo restante: {remaining_time:.1f}s")
            
            # Pequena pausa entre batches
            await asyncio.sleep(0.2)
        
        # Estatísticas finais
        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Migração em lote concluída em {total_time:.1f}s")
        logger.info(f"Total migrado: {len(migrated_urls)}/{total} documentos ({len(migrated_urls)/total*100:.1f}%)")
        logger.info(f"Estatísticas: {self._stats}")
        
        return migrated_urls

    async def close(self):
        """Fecha recursos"""
        if self.session:
            await self.session.close()
            self.session = None
            
        if self._connection_pool:
            await self._connection_pool.close()
            self._connection_pool = None
            
        # Fecha thread pool
        self.thread_pool.shutdown(wait=True)
        
        logger.info(f"Recursos fechados com sucesso. Estatísticas finais: {self._stats}")