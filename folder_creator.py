import asyncio
import logging
import json
import traceback
from typing import Dict, Optional, List
import aiohttp
import unicodedata
from folder_error_processor import FolderError, FolderErrorProcessor
from folder_name_validator import normalize_folder_name

logger = logging.getLogger(__name__)

class FolderCreator:
    def __init__(self, config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.folder_cache: Dict[str, Dict[int, Dict[str, int]]] = {}
        self.error_processor = FolderErrorProcessor()
        self.base_url = config.liferay_url

    @staticmethod
    def get_comparison_key(title: str) -> str:
        """Gera chave sem acentos para comparação"""
        return unicodedata.normalize('NFKD', title.lower()).encode('ascii', 'ignore').decode()

    def _get_folder_url(self, folder_type: str, parent_id: int = 0) -> str:
        """Gera URL para operações com pastas"""
        base_path = "structured-content-folders" if folder_type == 'journal' else "document-folders"
        
        if parent_id == 0:
            return f"{self.base_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/{base_path}"
        return f"{self.base_url}/o/headless-delivery/v1.0/{base_path}/{parent_id}/{base_path}"

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
            },
            connector=aiohttp.TCPConnector(ssl=False)
        )

    async def create_folder(self, title: str, parent_id: int = 0, folder_type: str = 'journal', hierarchy: List[str] = None) -> int:
        """Create a folder in Liferay"""
        if not self.session:
            await self.initialize_session()

        normalized_title = normalize_folder_name(title)
        if not normalized_title:
            logger.error(f"Nome de pasta inválido: {title}")
            return 0

        url = self._get_folder_url(folder_type, parent_id)
        description = f"Pasta de {'Conteúdo Estruturado' if folder_type == 'journal' else 'Documentos'}: {normalized_title}"

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
                
                self.error_processor.add_error(FolderError(
                    title=normalized_title,
                    folder_type=folder_type,
                    parent_id=parent_id,
                    hierarchy=hierarchy or [],
                    error_message=f"HTTP {response.status}: {response_text}"
                ))
                return 0
                
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

    async def ensure_folder_exists(self, title: str, parent_id: int = 0, folder_type: str = 'journal') -> int:
        """Ensure a folder exists, using cache if possible"""
        normalized_title = normalize_folder_name(title)
        comparison_key = self.get_comparison_key(normalized_title)
        
        # Verifica cache
        if normalized_title in self.folder_cache:
            if parent_id in self.folder_cache[normalized_title]:
                if folder_type in self.folder_cache[normalized_title][parent_id]:
                    cached_id = self.folder_cache[normalized_title][parent_id][folder_type]
                    logger.info(f"Pasta encontrada no cache: {normalized_title} (ID: {cached_id})")
                    return cached_id

        try:
            url = self._get_folder_url(folder_type, parent_id)
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    for folder in data.get('items', []):
                        if self.get_comparison_key(folder['name']) == comparison_key:
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
            if not hierarchy_levels or self.get_comparison_key(hierarchy_levels[-1]) != self.get_comparison_key(final_title):
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
            # Carrega erros atualizados do arquivo
            self.error_processor.load_errors()
            failed_folders = self.error_processor.get_failed_folders()
            
            if not failed_folders:
                logger.info("Nenhuma pasta para recriar")
                return

            logger.info(f"\nTentando recriar {len(failed_folders)} pastas que falharam...")
            
            # Cria uma nova lista para os erros que persistirem
            errors_to_keep = []
            processed_count = 0
            
            for error in failed_folders:
                # Pula se já tentou 3 vezes
                if error.retry_count >= 3:
                    logger.warning(f"Pasta '{error.title}' excedeu limite de tentativas ({error.retry_count})")
                    continue
                    
                processed_count += 1
                logger.info(f"Recriando: {error.title} (Tentativa {error.retry_count + 1}/3)")
                
                # Tenta recriar a pasta
                folder_id = await self.create_folder(
                    error.title,
                    error.parent_id,
                    error.folder_type,
                    error.hierarchy
                )
                
                # Se falhou, incrementa contador e mantém na lista
                if not folder_id:
                    error.retry_count += 1
                    if error.retry_count < 3:
                        errors_to_keep.append(error)
                    else:
                        logger.warning(f"Pasta '{error.title}' falhou em todas as tentativas")
                else:
                    logger.info(f"Pasta '{error.title}' criada com sucesso após {error.retry_count + 1} tentativas")
                
                # Aguarda antes da próxima tentativa
                await asyncio.sleep(2)
            
            # Atualiza a lista de erros
            self.error_processor.errors = errors_to_keep
            self.error_processor.save_errors()
            
            # Log do resultado final
            success_count = processed_count - len(errors_to_keep)
            if success_count > 0:
                logger.info(f"Recriadas com sucesso: {success_count} pastas")
            if errors_to_keep:
                logger.warning(f"Permanecem com erro: {len(errors_to_keep)} pastas")
            logger.info("Processo de retry concluído")
            
    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
            self.session = None