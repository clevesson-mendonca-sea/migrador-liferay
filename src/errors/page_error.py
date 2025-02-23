import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict

@dataclass
class PageError:
    title: str
    url: str
    parent_id: int
    hierarchy: List[str]
    error_message: str
    timestamp: str = datetime.now().isoformat()
    retry_count: int = 0

class ErrorTracker:
    def __init__(self):
        self.errors: List[PageError] = []
        self._current_file = f'migration_errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    
    def add_error(self, error: PageError):
        """Adiciona um erro à lista e salva imediatamente"""
        self.errors.append(error)
        self._save_errors()
    
    def get_failed_pages(self) -> List[PageError]:
        """Retorna lista de páginas que falharam"""
        return self.errors
    
    def _save_errors(self):
        """Salva os erros em um arquivo JSON"""
        try:
            error_data = {
                "total_errors": len(self.errors),
                "timestamp": datetime.now().isoformat(),
                "errors": [asdict(error) for error in self.errors]
            }
            
            with open(self._current_file, 'w', encoding='utf-8') as f:
                json.dump(error_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Erro ao salvar arquivo de erros: {str(e)}")