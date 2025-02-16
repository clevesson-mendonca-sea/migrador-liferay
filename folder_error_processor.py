import logging
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import List
from folder_name_validator import FolderNameValidator

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

class FolderErrorProcessor:
    def __init__(self, error_file="folder_migration_errors.json"):
        self.errors: List[FolderError] = []
        self.error_file = error_file
        self.folder_validator = FolderNameValidator()
        self.load_errors()

    def add_error(self, error: FolderError):
        """Adiciona um erro ao registro."""
        # Verifica se já existe um erro para esta pasta
        existing_error = next(
            (e for e in self.errors 
            if e.title == error.title and 
                e.folder_type == error.folder_type and 
                e.parent_id == error.parent_id),
            None
        )
        
        if existing_error:
            # Atualiza o erro existente
            existing_error.error_message = error.error_message
            existing_error.timestamp = error.timestamp
            existing_error.hierarchy = error.hierarchy
        else:
            # Adiciona novo erro
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