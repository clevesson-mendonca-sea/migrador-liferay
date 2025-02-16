from dataclasses import dataclass, field
import re
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
import os
import unicodedata
from dotenv import load_dotenv


from requests_cache import logger

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
                    self.errors = [
                        PageError(**e) for e in data
                    ]
        except Exception:
            self.errors = []

    def _save_errors(self):
        try:
            with open(self.error_file, 'w', encoding='utf-8') as f:
                json.dump([vars(e) for e in self.errors], f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def log_failed_pages(self, output_format: str = 'json'):
        """
        Gera um arquivo com páginas que falharam na migração.
        
        Args:
            output_format (str): Formato de saída ('json' ou 'txt')
        """
        if not self.errors:
            return
        
        # Cria diretório de logs se não existir
        os.makedirs('logs', exist_ok=True)
        
        # Gera nome de arquivo com timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format == 'json':
            # Gera arquivo JSON
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
        
        elif output_format == 'txt':
            # Gera arquivo de texto
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

def normalize_friendly_url(title: str) -> str:
    nfkd_form = unicodedata.normalize('NFKD', title)
    only_ascii = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    friendly_url = re.sub(r'[^a-zA-Z0-9-]', '-', only_ascii)
    friendly_url = re.sub(r'-+', '-', friendly_url).strip('-').lower()
    return friendly_url

def normalize_page_name(title: str) -> str:
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

class PageCreator:
    def __init__(self, config):
        self.config = config
        self.session = None
        self.page_cache = {}
        self.error_tracker = ErrorTracker()

    async def create_page(self, title: str, friendly_url: str, parent_id: int = 0, hierarchy: List[str] = None, page_type: str = "portlet", visible: bool = True, column_type: str = "1_column") -> int:
        normalized_title = normalize_page_name(title)
        normalized_url = normalize_friendly_url(friendly_url)
        hidden = str(not visible).lower()
        
        params = {
            "groupId": str(self.config.site_id),
            "privateLayout": "false",
            "parentLayoutId": str(parent_id),
            "name": normalized_title,
            "title": normalized_title,
            "description": "",
            "type": page_type,
            "hidden": hidden,
            "friendlyURL": f"/{normalized_url}",
        }
        
        try:
            async with self.session.post(
                f"{self.config.liferay_url}/api/jsonws/layout/add-layout",
                params=params
            ) as response:
                response_text = await response.text()
                if response.status in (200, 201):
                    result = await response.json()
                    page_id = result.get('layoutId') or result.get('plid')

                    if page_id:
                        update = {
                            "groupId": str(self.config.site_id),
                            "privateLayout": "false",
                            "layoutId": page_id,
                            "typeSettings": (
                                    "column-1=com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_\n"
                                    f"layout-template-id={column_type}\n"
                                    )
                         }

                        async with self.session.post(
                            f"{self.config.liferay_url}/api/jsonws/layout/update-layout",
                            params=update
                        ) as update_response:
                            update_response_text = await update_response.text()
                            if update_response.status in (200, 201):
                                print(f"Página criada e atualizada: {normalized_title} (ID: {page_id}) | Tipo: {page_type}")
                                return int(page_id)
                
        except Exception as e:
            error = PageError(
                title=normalized_title,
                url=normalized_url,
                parent_id=parent_id,
                hierarchy=hierarchy or [],
                error_message=str(e)
            )
            self.error_tracker.add_error(error)
        
        return 0
    
    async def ensure_page_exists(self, title: str, cache_key: str, parent_id: int = 0, friendly_url: str = "", hierarchy: List[str] = None, page_type: str ="portlet", visible: bool = True, column_type: str = "1_column") -> int:
        if cache_key in self.page_cache:
            return self.page_cache[cache_key]

        normalized_title = normalize_page_name(title)
        friendly_url = normalize_friendly_url(friendly_url)

        page_id = await self.create_page(normalized_title, friendly_url, parent_id, hierarchy, page_type, visible, column_type)
        
        if page_id:
            self.page_cache[cache_key] = page_id
            
        return page_id

    async def create_hierarchy(self, hierarchy: list, final_title: str, final_url: str, page_type: str = "widget", visible: bool = True, column_type: str = "1_column") -> int:
        current_path = ""
        parent_id = 0
        last_page_id = 0

        # Filtra 'Raiz' da hierarquia
        hierarchy_levels = [x for x in hierarchy if x.lower() != 'raiz']

        # Processa níveis da hierarquia
        for level in hierarchy_levels:
            normalized_level = normalize_page_name(level)
            current_path += f" > {normalized_level}" if current_path else normalized_level
            level_id = await self.ensure_page_exists(normalized_level, current_path, parent_id, final_url , hierarchy, page_type, visible, column_type)
            
            if level_id:
                parent_id = level_id
                last_page_id = level_id
            else:
                print(f"Falha ao criar nível: {normalized_level}")
                return 0

        # Cria página final se for diferente do último nível
        if (not hierarchy_levels or 
            normalize_page_name(final_title).lower() != normalize_page_name(hierarchy_levels[-1]).lower()):
            
            final_page_id = await self.create_page(
                normalize_page_name(final_title), 
                normalize_friendly_url(final_url), 
                parent_id, 
                hierarchy,
                page_type,
                visible,
                column_type
            )

            print(f"Página final criada: {final_title} (ID: {final_page_id}) Tipo da página {page_type}")
            
            if final_page_id:
                last_page_id = final_page_id

        return last_page_id
    
    async def retry_failed_pages(self):
        failed_pages = self.error_tracker.get_failed_pages()
        if not failed_pages:
            return
        
        for error in failed_pages:
            if error.retry_count < 3:
                error.retry_count += 1
                await self.create_page(error.title, error.url, error.parent_id, error.hierarchy)