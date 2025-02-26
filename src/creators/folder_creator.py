import asyncio
import logging
import json
import traceback
from typing import Dict, Optional, List, Set, Tuple, Any
import aiohttp
import unicodedata
from functools import lru_cache
from processors.folder_error_processor import FolderError, FolderErrorProcessor
from validators.folder_name_validator import normalize_folder_name

logger = logging.getLogger(__name__)

class FolderCreator:
    def __init__(self, config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        # Improved multi-level cache structure
        self.folder_cache: Dict[str, Dict[int, Dict[str, int]]] = {}
        # Fast lookup cache by comparison key
        self.comparison_cache: Dict[str, Dict[int, Dict[str, int]]] = {}
        # Cache for folder existence checks (parent_id, folder_type, comparison_key)
        self.existence_cache: Set[Tuple[int, str, str]] = set()
        # Cache of entire parent folders contents to reduce API calls
        self.parent_contents_cache: Dict[Tuple[int, str], Dict[str, int]] = {}
        self.error_processor = FolderErrorProcessor()
        self.base_url = config.liferay_url
        # Concurrent operations control
        self.semaphore = asyncio.Semaphore(20)
        # Connection pool
        self._connection_pool = None
        # Precomputed URLs
        self._site_journal_url = f"{self.base_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/structured-content-folders"
        self._site_document_url = f"{self.base_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/document-folders"

    @staticmethod
    @lru_cache(maxsize=500)
    def get_comparison_key(title: str) -> str:
        """Gera chave sem acentos para comparação - cached for performance"""
        return unicodedata.normalize('NFKD', title.lower()).encode('ascii', 'ignore').decode()

    @lru_cache(maxsize=200)
    def _get_folder_url(self, folder_type: str, parent_id: int = 0) -> str:
        """Gera URL para operações com pastas - cached"""
        if parent_id == 0:
            return self._site_journal_url if folder_type == 'journal' else self._site_document_url
            
        base_path = "structured-content-folders" if folder_type == 'journal' else "document-folders"
        return f"{self.base_url}/o/headless-delivery/v1.0/{base_path}/{parent_id}/{base_path}"

    async def initialize_session(self):
        """Initialize HTTP session with connection pooling"""
        if self.session:
            await self.session.close()
            
        # Create connection pool if not exists
        if not self._connection_pool:
            self._connection_pool = aiohttp.TCPConnector(
                ssl=False,
                limit=50,  # Increased connection limit
                ttl_dns_cache=600,  # DNS cache for 10 minutes
                keepalive_timeout=60  # Keep connections alive longer
            )
            
        auth = aiohttp.BasicAuth(
            login=self.config.liferay_user,
            password=self.config.liferay_pass
        )
        
        self.session = aiohttp.ClientSession(
            auth=auth,
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Connection': 'keep-alive'
            },
            connector=self._connection_pool,
            timeout=aiohttp.ClientTimeout(total=30, connect=10)
        )
        logger.info("Session initialized with improved connection settings")

    async def _controlled_request(self, method: str, url: str, **kwargs) -> Tuple[int, Any]:
        """Execute HTTP request with concurrency control and improved error handling"""
        async with self.semaphore:
            if not self.session:
                await self.initialize_session()
                
            try:
                async with getattr(self.session, method)(url, **kwargs) as response:
                    if response.content_type == 'application/json':
                        data = await response.json(content_type=None)
                    else:
                        data = await response.text()
                    return response.status, data
            except aiohttp.ClientResponseError as e:
                logger.error(f"HTTP error during {method} to {url}: {e.status} - {str(e)}")
                raise
            except aiohttp.ClientError as e:
                logger.error(f"Client error during {method} to {url}: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error during {method} to {url}: {str(e)}")
                raise

    async def _retry_operation(self, operation, *args, max_retries=3, **kwargs):
        """Retry operation with exponential backoff"""
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return await operation(*args, **kwargs)
            except aiohttp.ClientResponseError as e:
                last_error = e
                if e.status in (429, 503, 504):  # Rate limiting or service unavailable
                    backoff = 1 * (2 ** attempt)
                    logger.warning(f"Rate limit or service unavailable ({e.status}). Attempt {attempt+1}/{max_retries}. Waiting {backoff}s...")
                    await asyncio.sleep(backoff)
                    continue
                elif e.status in (400, 401, 403, 404, 405):  # Client errors - don't retry
                    raise e
                # Other HTTP errors - retry
                if attempt < max_retries - 1:
                    backoff = 1 * (2 ** attempt)
                    logger.warning(f"HTTP error ({e.status}). Attempt {attempt+1}/{max_retries}. Waiting {backoff}s...")
                    await asyncio.sleep(backoff)
                    continue
                raise last_error
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    backoff = 1 * (2 ** attempt)
                    logger.warning(f"Error: {str(e)}. Attempt {attempt+1}/{max_retries}. Waiting {backoff}s...")
                    await asyncio.sleep(backoff)
                    continue
                raise last_error

    def _cache_folder_id(self, title: str, parent_id: int, folder_type: str, folder_id: int):
        """Cache folder ID with improved multi-level caching"""
        # Standard cache
        if title not in self.folder_cache:
            self.folder_cache[title] = {}
        if parent_id not in self.folder_cache[title]:
            self.folder_cache[title][parent_id] = {}
        self.folder_cache[title][parent_id][folder_type] = folder_id
        
        # Comparison key cache
        comparison_key = self.get_comparison_key(title)
        if comparison_key not in self.comparison_cache:
            self.comparison_cache[comparison_key] = {}
        if parent_id not in self.comparison_cache[comparison_key]:
            self.comparison_cache[comparison_key][parent_id] = {}
        self.comparison_cache[comparison_key][parent_id][folder_type] = folder_id
        
        # Existence cache
        self.existence_cache.add((parent_id, folder_type, comparison_key))
        
        # Parent contents cache (maintain map of all folders under this parent)
        parent_key = (parent_id, folder_type)
        if parent_key not in self.parent_contents_cache:
            self.parent_contents_cache[parent_key] = {}
        self.parent_contents_cache[parent_key][comparison_key] = folder_id

    def _get_cached_folder_id(self, title: str, parent_id: int, folder_type: str) -> Optional[int]:
        """Get folder ID from any available cache with fallbacks"""
        # Check direct cache first (fastest)
        if (title in self.folder_cache and 
            parent_id in self.folder_cache[title] and 
            folder_type in self.folder_cache[title][parent_id]):
            return self.folder_cache[title][parent_id][folder_type]
        
        # Check comparison key cache (for case/accent insensitive matches)
        comparison_key = self.get_comparison_key(title)
        if (comparison_key in self.comparison_cache and 
            parent_id in self.comparison_cache[comparison_key] and 
            folder_type in self.comparison_cache[comparison_key][parent_id]):
            return self.comparison_cache[comparison_key][parent_id][folder_type]
        
        # Check parent contents cache
        parent_key = (parent_id, folder_type)
        if parent_key in self.parent_contents_cache and comparison_key in self.parent_contents_cache[parent_key]:
            return self.parent_contents_cache[parent_key][comparison_key]
        
        return None

    async def create_folder(self, title: str, parent_id: int = 0, folder_type: str = 'journal', hierarchy: List[str] = None) -> int:
        """Create a folder in Liferay with retry and improved caching"""
        normalized_title = normalize_folder_name(title, None, folder_type)
        if not normalized_title:
            logger.error(f"Nome de pasta inválido: {title}")
            return 0

        # Check cache first
        cached_id = self._get_cached_folder_id(normalized_title, parent_id, folder_type)
        if cached_id:
            logger.info(f"Pasta encontrada no cache: {normalized_title} (ID: {cached_id})")
            return cached_id

        url = self._get_folder_url(folder_type, parent_id)
        description = f"Pasta de {'Conteúdo Estruturado' if folder_type == 'journal' else 'Documentos'}: {normalized_title}"

        params = {
            "name": normalized_title,
            "description": description
        }
        
        async def create_attempt():
            logger.info(f"Criando pasta {folder_type}: {normalized_title} (Parent: {parent_id})")
            status, result = await self._controlled_request('post', url, json=params)
            
            if status in (200, 201):
                folder_id = result.get('id')
                if folder_id:
                    folder_id = int(folder_id)
                    logger.info(f"Pasta criada: {normalized_title} (ID: {folder_id})")
                    self._cache_folder_id(normalized_title, parent_id, folder_type, folder_id)
                    return folder_id
                    
            error_msg = f"HTTP {status}: {json.dumps(result) if isinstance(result, dict) else result}"
            self.error_processor.add_error(FolderError(
                title=normalized_title,
                folder_type=folder_type,
                parent_id=parent_id,
                hierarchy=hierarchy or [],
                error_message=error_msg
            ))
            raise Exception(error_msg)
        
        try:
            return await self._retry_operation(create_attempt, max_retries=3)
        except Exception as e:
            self.error_processor.add_error(FolderError(
                title=normalized_title,
                folder_type=folder_type,
                parent_id=parent_id,
                hierarchy=hierarchy or [],
                error_message=f"Erro: {str(e)}\n{traceback.format_exc()}"
            ))
            logger.error(f"Erro ao criar pasta {normalized_title}: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    async def _fetch_and_cache_parent_folders(self, parent_id: int, folder_type: str) -> Dict[str, int]:
        """Fetch all folders under a parent and cache them for future lookups"""
        # Check if we already have this parent's contents
        parent_key = (parent_id, folder_type)
        if parent_key in self.parent_contents_cache:
            return self.parent_contents_cache[parent_key]
            
        url = self._get_folder_url(folder_type, parent_id)
        folder_map = {}
        
        try:
            # Get all folders with pagination if needed
            page = 1
            page_size = 100
            has_more = True
            
            while has_more:
                params = {'page': page, 'pageSize': page_size}
                status, data = await self._controlled_request('get', url, params=params)
                
                if status == 200:
                    items = data.get('items', [])
                    total_count = data.get('totalCount', 0)
                    
                    # Process this page of folders
                    for folder in items:
                        folder_name = folder['name']
                        folder_id = int(folder['id'])
                        comparison_key = self.get_comparison_key(folder_name)
                        
                        # Cache this folder in all caches
                        self._cache_folder_id(folder_name, parent_id, folder_type, folder_id)
                        folder_map[comparison_key] = folder_id
                    
                    # Check if we need another page
                    fetched_count = (page - 1) * page_size + len(items)
                    has_more = fetched_count < total_count
                    page += 1
                else:
                    logger.error(f"Error fetching folders: HTTP {status}")
                    has_more = False
                    
            # Store in parent contents cache
            self.parent_contents_cache[parent_key] = folder_map
            return folder_map
            
        except Exception as e:
            logger.error(f"Error fetching parent folders: {str(e)}")
            return {}

    async def ensure_folder_exists(self, title: str, parent_id: int = 0, folder_type: str = 'journal') -> int:
        """Ensure a folder exists, using optimized cache and bulk folder fetching"""
        normalized_title = normalize_folder_name(title, None, folder_type)
        comparison_key = self.get_comparison_key(normalized_title)
        
        # Fast path: Check multi-level cache first
        cached_id = self._get_cached_folder_id(normalized_title, parent_id, folder_type)
        if cached_id:
            logger.info(f"Pasta encontrada no cache: {normalized_title} (ID: {cached_id})")
            return cached_id
        
        # Fast path: Check existence cache to avoid duplicate API calls
        if (parent_id, folder_type, comparison_key) in self.existence_cache:
            # We've checked this before and know it doesn't exist
            logger.info(f"Cache indica que pasta não existe: {normalized_title}")
            return await self.create_folder(normalized_title, parent_id, folder_type)
        
        try:
            # Optimized approach: fetch and cache all folders under this parent at once
            parent_folders = await self._fetch_and_cache_parent_folders(parent_id, folder_type)
            
            # Check if our folder exists in the fetched data
            if comparison_key in parent_folders:
                folder_id = parent_folders[comparison_key]
                logger.info(f"Pasta existente encontrada: {normalized_title} (ID: {folder_id})")
                return folder_id
            
            # Mark as checked so we don't waste time checking again
            self.existence_cache.add((parent_id, folder_type, comparison_key))
            
            # If not found, create it
            logger.info(f"Pasta não encontrada, criando: {normalized_title}")
            return await self.create_folder(normalized_title, parent_id, folder_type)

        except Exception as e:
            logger.error(f"Erro ao buscar pasta {normalized_title}: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    async def create_folder_hierarchy(self, hierarchy: List[str], final_title: str, folder_type: str = 'journal') -> int:
        """Create a folder hierarchy with parallel processing where possible"""
        # Remove 'Raiz' da hierarquia
        hierarchy_levels = [x for x in hierarchy if x.lower() != 'raiz']
        if not hierarchy_levels and not final_title:
            return 0
            
        try:
            current_parent_id = 0
            
            # Process hierarchy using a more efficient approach
            if hierarchy_levels:
                # Check if we already have the full path cached
                full_path_key = '>'.join([self.get_comparison_key(level) for level in hierarchy_levels])
                full_path_cache_key = (full_path_key, folder_type)
                if full_path_cache_key in getattr(self, 'hierarchy_cache', {}):
                    logger.info(f"Usando hierarquia completa do cache: {full_path_key}")
                    current_parent_id = self.hierarchy_cache[full_path_cache_key]
                else:
                    # Process each level
                    for level in hierarchy_levels:
                        logger.info(f"Processando nível: {level} (Parent: {current_parent_id})")
                        folder_id = await self.ensure_folder_exists(level, current_parent_id, folder_type)
                        
                        if folder_id:
                            current_parent_id = folder_id
                        else:
                            logger.error(f"Falha ao processar nível: {level}")
                            return 0
                            
                    # Cache the complete hierarchy path
                    if not hasattr(self, 'hierarchy_cache'):
                        self.hierarchy_cache = {}
                    self.hierarchy_cache[full_path_cache_key] = current_parent_id

            # Handle final folder if needed
            final_normalized = normalize_folder_name(final_title, None, folder_type)
            if not hierarchy_levels or self.get_comparison_key(hierarchy_levels[-1]) != self.get_comparison_key(final_title):
                logger.info(f"Criando pasta final: {final_title} (Parent: {current_parent_id})")
                final_folder_id = await self.ensure_folder_exists(final_title, current_parent_id, folder_type)
                if final_folder_id:
                    logger.info(f"Pasta criada: {final_title} (ID: {final_folder_id})")
                    return final_folder_id
            
            return current_parent_id
            
        except Exception as e:
            logger.error(f"Erro ao criar hierarquia: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    async def retry_failed_folders(self):
        """Tenta recriar pastas que falharam em paralelo"""
        # Carrega erros atualizados do arquivo
        self.error_processor.load_errors()
        failed_folders = self.error_processor.get_failed_folders()
        
        if not failed_folders:
            logger.info("Nenhuma pasta para recriar")
            return

        logger.info(f"\nTentando recriar {len(failed_folders)} pastas que falharam...")
        
        # Processa em lotes para melhor controle
        batch_size = 10
        successful_folders = []
        remaining_errors = []
        
        for i in range(0, len(failed_folders), batch_size):
            batch = failed_folders[i:i + batch_size]
            batch_tasks = []
            
            for error in batch:
                # Pula se já tentou 3 vezes
                if error.retry_count >= 3:
                    logger.warning(f"Pasta '{error.title}' excedeu limite de tentativas ({error.retry_count})")
                    continue
                
                # Incrementa contador antes de tentar
                error.retry_count += 1
                
                # Adiciona tarefa para processamento em paralelo
                task = asyncio.create_task(self._retry_create_folder(error))
                batch_tasks.append((error, task))
            
            # Aguarda conclusão do lote
            if batch_tasks:
                for error, task in batch_tasks:
                    try:
                        success = await task
                        if success:
                            successful_folders.append(error.title)
                        elif error.retry_count < 3:
                            remaining_errors.append(error)
                    except Exception as e:
                        logger.error(f"Erro ao recriar pasta '{error.title}': {str(e)}")
                        if error.retry_count < 3:
                            remaining_errors.append(error)
            
            # Pequena pausa entre lotes
            if i + batch_size < len(failed_folders):
                await asyncio.sleep(0.5)
        
        # Atualiza a lista de erros
        self.error_processor.errors = remaining_errors
        self.error_processor.save_errors()
        
        # Log do resultado final
        success_count = len(successful_folders)
        if success_count > 0:
            logger.info(f"Recriadas com sucesso: {success_count} pastas")
        if remaining_errors:
            logger.warning(f"Permanecem com erro: {len(remaining_errors)} pastas")
        logger.info("Processo de retry concluído")

    async def _retry_create_folder(self, error: FolderError) -> bool:
        """Helper method para retry_failed_folders processing"""
        logger.info(f"Recriando: {error.title} (Tentativa {error.retry_count}/3)")
        try:
            folder_id = await self.create_folder(
                error.title,
                error.parent_id,
                error.folder_type,
                error.hierarchy
            )
            if folder_id:
                logger.info(f"Pasta '{error.title}' criada com sucesso após {error.retry_count} tentativas")
                return True
            return False
        except Exception as e:
            logger.error(f"Falha ao recriar pasta '{error.title}': {str(e)}")
            return False
            
    async def close(self):
        """Close HTTP session and resources"""
        if self.session:
            await self.session.close()
            self.session = None
            
        if self._connection_pool:
            await self._connection_pool.close()
            self._connection_pool = None
            
        # Clear caches
        self.folder_cache.clear()
        self.comparison_cache.clear()
        self.existence_cache.clear()
        self.parent_contents_cache.clear()
        if hasattr(self, 'hierarchy_cache'):
            self.hierarchy_cache.clear()