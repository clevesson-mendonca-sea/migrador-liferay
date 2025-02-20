from dataclasses import dataclass, field
from typing import List
from datetime import datetime
import json
import os

@dataclass
class PageError:
    title: str
    url: str = ''
    parent_id: int = 0
    hierarchy: List[str] = field(default_factory=list)
    error_message: str = ''
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    retry_count: int = 0

class ErrorTracker:
    def __init__(self, error_file="migration_errors.json"):
        self.errors: List[PageError] = []
        self.error_file = error_file
        self._load_errors()

    def add_error(self, error: PageError):
        self.errors.append(error)
        self._save_errors()

    def _load_errors(self):
        try:
            if os.path.exists(self.error_file):
                with open(self.error_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.errors = [PageError(**e) for e in data]
        except Exception:
            self.errors = []

    def _save_errors(self):
        try:
            with open(self.error_file, 'w', encoding='utf-8') as f:
                json.dump([vars(e) for e in self.errors], f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def log_failed_pages(self, output_format: str = 'json'):
        if not self.errors:
            return
        
        os.makedirs('logs', exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format == 'json':
            self._log_json(timestamp)
        elif output_format == 'txt':
            self._log_txt(timestamp)

    def _log_json(self, timestamp: str):
        filename = f'logs/failed_pages_{timestamp}.json'
        failed_pages_data = [
            {
                'title': error.title,
                'url': error.url,
                'hierarchy': error.hierarchy,
                'error_message': error.error_message,
                'timestamp': error.timestamp
            } for error in self.errors
        ]
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(failed_pages_data, f, indent=2, ensure_ascii=False)
        
        print(f"Páginas com falha salvas em {filename}")

    def _log_txt(self, timestamp: str):
        filename = f'logs/failed_pages_{timestamp}.txt'
        
        with open(filename, 'w', encoding='utf-8') as f:
            for error in self.errors:
                f.write(f"Título: {error.title}\n")
                f.write(f"URL: {error.url}\n")
                f.write(f"Hierarquia: {' > '.join(error.hierarchy)}\n")
                f.write(f"Erro: {error.error_message}\n")
                f.write(f"Timestamp: {error.timestamp}\n")
                f.write("-" * 50 + "\n")
        
        print(f"Páginas com falha salvas em {filename}")

    def get_failed_pages(self) -> List[PageError]:
        return self.errors

    def clear_errors(self):
        self.errors = []
        self._save_errors()