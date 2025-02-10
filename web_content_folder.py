import asyncio
import logging
import json
import traceback
from typing import List, Dict, Optional
import aiohttp
import os
from dataclasses import dataclass, field
from datetime import datetime
import unicodedata

logger = logging.getLogger(__name__)

@dataclass
class FolderError:
    title: str
    folder_type: str  # 'journal' ou 'documents'
    parent_id: int
    hierarchy: List[str]
    error_message: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    retry_count: int = 0

class FolderErrorTracker:
    def __init__(self, error_file="folder_migration_errors.json"):
        self.errors: List[FolderError] = []
        self.error_file = error_file
        self.load_errors()

    def add_error(self, error: FolderError):
        """Adiciona um erro ao registro."""
        self.errors.append(error)
        self.save_errors()
        logger.error(f"Erro de pasta registrado: {error.title} ({error.folder_type}) - {error.error_message}")

    def load_errors(self):
        """Carrega erros do arquivo."""
        try:
            if os.path.exists(self.error_file):
                with open(self.error_file, 'r') as f:
                    data = json.load(f)
                    self.errors = [
                        FolderError(
                            title=e.get('title', ''),
                            folder_type=e.get('folder_type', 'journal'),
                            parent_id=e.get('parent_id', 0),
                            hierarchy=e.get('hierarchy', []),
                            error_message=e.get('error_message', ''),
                            timestamp=e.get('timestamp', datetime.now().isoformat()),
                            retry_count=e.get('retry_count', 0)
                        ) for e in data
                    ]
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo de erros de pasta: {e}")
            self.errors = []

    def save_errors(self):
        """Salva erros em arquivo."""
        try:
            with open(self.error_file, 'w') as f:
                json.dump([
                    {
                        'title': e.title,
                        'folder_type': e.folder_type,
                        'parent_id': e.parent_id,
                        'hierarchy': e.hierarchy,
                        'error_message': e.error_message,
                        'timestamp': e.timestamp,
                        'retry_count': e.retry_count
                    } for e in self.errors
                ], f, indent=2)
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo de erros de pasta: {e}")

    def get_failed_folders(self) -> List[FolderError]:
        """Retorna pastas que falharam na criação."""
        return self.errors

    def clear_errors(self):
        """Limpa todos os erros."""
        self.errors = []
        self.save_errors()

def normalize_folder_name(title: str) -> str:
    """Normaliza o nome mantendo acentos"""
    # Palavras especiais
    cases = {
        'lower': {'de', 'da', 'do', 'das', 'dos', 'e', 'é', 'em'},
        'upper': {'df', 'gdf', 'sei', 'cig'}
    }
    
    words = title.strip().split()
    if not words:
        return ''
        
    def format_word(word: str, index: int) -> str:
        word = word.lower()
        if word in cases['upper']: return word.upper()
        if word in cases['lower'] and index > 0: return word
        return word.capitalize()
    
    return ' '.join(format_word(w, i) for i, w in enumerate(words))

def get_comparison_key(title: str) -> str:
    """Gera chave sem acentos para comparação"""
    return unicodedata.normalize('NFKD', title.lower()).encode('ascii', 'ignore').decode()

class FolderCreator:
    def __init__(self, config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.folder_cache: Dict[str, Dict[int, Dict[str, int]]] = {}
        self.error_tracker = FolderErrorTracker()

    async def initialize_session(self):
        """Initialize HTTP session"""
        if self.session:
            await self.session.close()
            
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

    async def create_folder(self, title: str, parent_id: int = 0, folder_type: str = 'journal', hierarchy: List[str] = None) -> int:
        """Create a folder in Liferay"""
        if not self.session:
            await self.initialize_session()

        normalized_title = normalize_folder_name(title)
        
        # Escolhe o endpoint e a descrição baseado no tipo de pasta
        if folder_type == 'journal':
            url = (f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/structured-content-folders" 
                if parent_id == 0 
                else f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{parent_id}/structured-content-folders")
            description = f"Pasta de Conteúdo Estruturado: {normalized_title}"
        elif folder_type == 'documents':
            url = (f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/document-folders" 
                if parent_id == 0
                else f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{parent_id}/document-folders")
            description = f"Pasta de Documentos: {normalized_title}"
        else:
            logger.error(f"Tipo de pasta não suportado: {folder_type}")
            return 0

        params = {
            "name": normalized_title,
            "description": description
        }
        
        try:
            logger.info(f"Criando pasta {folder_type}: {normalized_title} (Parent: {parent_id})")
            
            async with self.session.post(url, json=params) as response:
                response_text = await response.text()
                
                if response.status in (200, 201):
                    result = json.loads(response_text)
                    folder_id = result.get('id')
                    if folder_id:
                        logger.info(f"Pasta criada: {normalized_title} (ID: {folder_id})")
                        return int(folder_id)
                
                error = FolderError(
                    title=normalized_title,
                    folder_type=folder_type,
                    parent_id=parent_id,
                    hierarchy=hierarchy or [],
                    error_message=f"HTTP {response.status}: {response_text}"
                )
                self.error_tracker.add_error(error)
                return 0
                
        except Exception as e:
            error = FolderError(
                title=normalized_title,
                folder_type=folder_type,
                parent_id=parent_id,
                hierarchy=hierarchy or [],
                error_message=f"Erro: {str(e)}\n{traceback.format_exc()}"
            )
            self.error_tracker.add_error(error)
            logger.error(f"Erro ao criar pasta {normalized_title}: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    async def ensure_folder_exists(self, title: str, parent_id: int = 0, folder_type: str = 'journal') -> int:
        """Ensure a folder exists, using cache if possible"""
        normalized_title = normalize_folder_name(title)
        comparison_key = get_comparison_key(normalized_title)
        
        # Verifica cache
        if normalized_title in self.folder_cache:
            if parent_id in self.folder_cache[normalized_title]:
                if folder_type in self.folder_cache[normalized_title][parent_id]:
                    cached_id = self.folder_cache[normalized_title][parent_id][folder_type]
                    logger.info(f"Pasta encontrada no cache: {normalized_title} (ID: {cached_id})")
                    return cached_id

        try:
            # Escolhe URL baseado no tipo de pasta
            if folder_type == 'journal':
                url = (f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/structured-content-folders" 
                    if parent_id == 0 
                    else f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{parent_id}/structured-content-folders")
            else:
                url = (f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/document-folders" 
                    if parent_id == 0
                    else f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{parent_id}/document-folders")
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    for folder in data.get('items', []):
                        if get_comparison_key(folder['name']) == comparison_key:
                            folder_id = folder['id']
                            # Atualiza cache
                            if normalized_title not in self.folder_cache:
                                self.folder_cache[normalized_title] = {}
                            if parent_id not in self.folder_cache[normalized_title]:
                                self.folder_cache[normalized_title][parent_id] = {}
                            self.folder_cache[normalized_title][parent_id][folder_type] = folder_id
                            logger.info(f"Pasta existente encontrada: {normalized_title} (ID: {folder_id})")
                            return folder_id
            
            # Se não encontrou, cria
            folder_id = await self.create_folder(normalized_title, parent_id, folder_type)
            if folder_id:
                if normalized_title not in self.folder_cache:
                    self.folder_cache[normalized_title] = {}
                if parent_id not in self.folder_cache[normalized_title]:
                    self.folder_cache[normalized_title][parent_id] = {}
                self.folder_cache[normalized_title][parent_id][folder_type] = folder_id
            return folder_id

        except Exception as e:
            logger.error(f"Erro ao buscar pasta {normalized_title}: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    async def create_folder_hierarchy(self, hierarchy: List[str], final_title: str, folder_type: str = 'journal') -> int:
        """Create a folder hierarchy"""
        current_parent_id = 0
        current_folder_id = 0

        # Remove 'Raiz' da hierarquia
        hierarchy_levels = [x for x in hierarchy if x.lower() != 'raiz']
        
        try:
            # Processa cada nível da hierarquia
            for level in hierarchy_levels:
                logger.info(f"Processando nível: {level} (Parent: {current_parent_id})")
                current_folder_id = await self.ensure_folder_exists(level, current_parent_id, folder_type)
                
                if current_folder_id:
                    current_parent_id = current_folder_id
                else:
                    logger.error(f"Falha ao processar nível: {level}")
                    return 0

            # Se o título final é diferente do último nível, cria pasta adicional
            final_normalized = normalize_folder_name(final_title)
            if not hierarchy_levels or get_comparison_key(hierarchy_levels[-1]) != get_comparison_key(final_title):
                logger.info(f"Criando pasta final: {final_title} (Parent: {current_parent_id})")
                final_folder_id = await self.ensure_folder_exists(final_title, current_parent_id, folder_type)
                if final_folder_id:
                    logger.info(f"Pasta criada: {final_title} (ID: {final_folder_id})")
                    return final_folder_id
            
            return current_folder_id
            
        except Exception as e:
            logger.error(f"Erro ao criar hierarquia: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    async def retry_failed_folders(self):
        """Tenta recriar pastas que falharam"""
        failed_folders = self.error_tracker.get_failed_folders()
        if not failed_folders:
            return

        logger.info(f"\nTentando recriar {len(failed_folders)} pastas que falharam...")
        
        for error in failed_folders:
            if error.retry_count < 3:
                logger.info(f"Recriando: {error.title}")
                error.retry_count += 1
                await self.create_folder(
                    error.title,
                    error.parent_id,
                    error.folder_type,
                    error.hierarchy
                )
                await asyncio.sleep(2 ** error.retry_count)  # Backoff exponencial

    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
            self.session = None
            
def get_comparison_key(title: str) -> str:
    """Gera chave sem acentos para comparação"""
    return unicodedata.normalize('NFKD', title.lower()).encode('ascii', 'ignore').decode()

class FolderCreator:
    def __init__(self, config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.folder_cache: Dict[str, Dict[int, Dict[str, int]]] = {}
        self.error_tracker = FolderErrorTracker()

    async def initialize_session(self):
        """Initialize HTTP session"""
        if self.session:
            await self.session.close()
            
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

    async def create_folder(self, title: str, parent_id: int = 0, folder_type: str = 'journal', hierarchy: List[str] = None) -> int:
        """Create a folder in Liferay"""
        if not self.session:
            await self.initialize_session()

        normalized_title = normalize_folder_name(title)
        
        # Escolhe o endpoint e a descrição baseado no tipo de pasta
        if folder_type == 'journal':
            url = (f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/structured-content-folders" 
                if parent_id == 0 
                else f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{parent_id}/structured-content-folders")
            description = f"Pasta de Conteúdo Estruturado: {normalized_title}"
        elif folder_type == 'documents':
            url = (f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/document-folders" 
                if parent_id == 0
                else f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{parent_id}/document-folders")
            description = f"Pasta de Documentos: {normalized_title}"
        else:
            logger.error(f"Tipo de pasta não suportado: {folder_type}")
            return 0

        params = {
            "name": normalized_title,
            "description": description
        }
        
        try:
            logger.info(f"Criando pasta {folder_type}: {normalized_title} (Parent: {parent_id})")
            
            async with self.session.post(url, json=params) as response:
                response_text = await response.text()
                
                if response.status in (200, 201):
                    result = json.loads(response_text)
                    folder_id = result.get('id')
                    if folder_id:
                        logger.info(f"Pasta criada: {normalized_title} (ID: {folder_id})")
                        return int(folder_id)
                
                error = FolderError(
                    title=normalized_title,
                    folder_type=folder_type,
                    parent_id=parent_id,
                    hierarchy=hierarchy or [],
                    error_message=f"HTTP {response.status}: {response_text}"
                )
                self.error_tracker.add_error(error)
                return 0
                
        except Exception as e:
            error = FolderError(
                title=normalized_title,
                folder_type=folder_type,
                parent_id=parent_id,
                hierarchy=hierarchy or [],
                error_message=f"Erro: {str(e)}\n{traceback.format_exc()}"
            )
            self.error_tracker.add_error(error)
            logger.error(f"Erro ao criar pasta {normalized_title}: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    async def ensure_folder_exists(self, title: str, parent_id: int = 0, folder_type: str = 'journal') -> int:
        """Ensure a folder exists, using cache if possible"""
        normalized_title = normalize_folder_name(title)
        comparison_key = get_comparison_key(normalized_title)
        
        # Verifica cache
        if normalized_title in self.folder_cache:
            if parent_id in self.folder_cache[normalized_title]:
                if folder_type in self.folder_cache[normalized_title][parent_id]:
                    cached_id = self.folder_cache[normalized_title][parent_id][folder_type]
                    logger.info(f"Pasta encontrada no cache: {normalized_title} (ID: {cached_id})")
                    return cached_id

        try:
            # Escolhe URL baseado no tipo de pasta
            if folder_type == 'journal':
                url = (f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/structured-content-folders" 
                    if parent_id == 0 
                    else f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{parent_id}/structured-content-folders")
            else:
                url = (f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/document-folders" 
                    if parent_id == 0
                    else f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{parent_id}/document-folders")
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    for folder in data.get('items', []):
                        if get_comparison_key(folder['name']) == comparison_key:
                            folder_id = folder['id']
                            # Atualiza cache
                            if normalized_title not in self.folder_cache:
                                self.folder_cache[normalized_title] = {}
                            if parent_id not in self.folder_cache[normalized_title]:
                                self.folder_cache[normalized_title][parent_id] = {}
                            self.folder_cache[normalized_title][parent_id][folder_type] = folder_id
                            logger.info(f"Pasta existente encontrada: {normalized_title} (ID: {folder_id})")
                            return folder_id
            
            # Se não encontrou, cria
            folder_id = await self.create_folder(normalized_title, parent_id, folder_type)
            if folder_id:
                if normalized_title not in self.folder_cache:
                    self.folder_cache[normalized_title] = {}
                if parent_id not in self.folder_cache[normalized_title]:
                    self.folder_cache[normalized_title][parent_id] = {}
                self.folder_cache[normalized_title][parent_id][folder_type] = folder_id
            return folder_id

        except Exception as e:
            logger.error(f"Erro ao buscar pasta {normalized_title}: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    async def create_folder_hierarchy(self, hierarchy: List[str], final_title: str, folder_type: str = 'journal') -> int:
        """Create a folder hierarchy"""
        current_parent_id = 0
        current_folder_id = 0

        # Remove 'Raiz' da hierarquia
        hierarchy_levels = [x for x in hierarchy if x.lower() != 'raiz']
        
        try:
            # Processa cada nível da hierarquia
            for level in hierarchy_levels:
                logger.info(f"Processando nível: {level} (Parent: {current_parent_id})")
                current_folder_id = await self.ensure_folder_exists(level, current_parent_id, folder_type)
                
                if current_folder_id:
                    current_parent_id = current_folder_id
                else:
                    logger.error(f"Falha ao processar nível: {level}")
                    return 0

            # Se o título final é diferente do último nível, cria pasta adicional
            final_normalized = normalize_folder_name(final_title)
            if not hierarchy_levels or get_comparison_key(hierarchy_levels[-1]) != get_comparison_key(final_title):
                logger.info(f"Criando pasta final: {final_title} (Parent: {current_parent_id})")
                final_folder_id = await self.ensure_folder_exists(final_title, current_parent_id, folder_type)
                if final_folder_id:
                    logger.info(f"Pasta criada: {final_title} (ID: {final_folder_id})")
                    return final_folder_id
            
            return current_folder_id
            
        except Exception as e:
            logger.error(f"Erro ao criar hierarquia: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    async def retry_failed_folders(self):
        """Tenta recriar pastas que falharam"""
        failed_folders = self.error_tracker.get_failed_folders()
        if not failed_folders:
            return

        logger.info(f"\nTentando recriar {len(failed_folders)} pastas que falharam...")
        
        for error in failed_folders:
            if error.retry_count < 3:
                logger.info(f"Recriando: {error.title}")
                error.retry_count += 1
                await self.create_folder(
                    error.title,
                    error.parent_id,
                    error.folder_type,
                    error.hierarchy
                )
                await asyncio.sleep(2 ** error.retry_count)  # Backoff exponencial

    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
            self.session = None