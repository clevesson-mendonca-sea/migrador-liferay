import random
from typing import List
from page_error import ErrorTracker, PageError
from page_processor import PageProcessor

class PageCreator:
    def __init__(self, config):
        self.config = config
        self.session = None
        self.page_cache = {}
        self.error_tracker = ErrorTracker()
        self.processor = PageProcessor()

    async def create_page(self, title: str, friendly_url: str, parent_id: int = 0, 
                         hierarchy: List[str] = None, page_type: str = "portlet", 
                         visible: bool = True, column_type: str = "1_column") -> int:
        try:
            normalized_title = self.processor.normalize_page_name(title)
            normalized_url = self.processor.normalize_friendly_url(friendly_url)
            page_id = await self._create_page_request(normalized_title, normalized_url, 
                                                    parent_id, visible, page_type)
            
            if page_id:
                await self._update_page_layout(page_id, column_type)
                print(f"Página criada e atualizada: {normalized_title} (ID: {page_id}) | Tipo: {page_type}")
                return int(page_id)
            
        except Exception as e:
            self._handle_page_creation_error(normalized_title, normalized_url, 
                                           parent_id, hierarchy, str(e))
        return 0

    async def _create_page_request(self, title: str, url: str, parent_id: int, 
                                 visible: bool, page_type: str):
        params = {
            "groupId": str(self.config.site_id),
            "privateLayout": "false",
            "parentLayoutId": str(parent_id),
            "name": title,
            "title": title,
            "description": "",
            "type": page_type,
            "hidden": str(not visible).lower(),
            "friendlyURL": f"/{url}",
        }
        
        async with self.session.post(
            f"{self.config.liferay_url}/api/jsonws/layout/add-layout",
            params=params
        ) as response:
            if response.status in (200, 201):
                result = await response.json()
                return result.get('layoutId') or result.get('plid')
            return 0

    def _handle_page_creation_error(self, title: str, url: str, parent_id: int, 
                                  hierarchy: List[str], error_message: str):
        error = PageError(
            title=title,
            url=url,
            parent_id=parent_id,
            hierarchy=hierarchy or [],
            error_message=error_message
        )
        self.error_tracker.add_error(error)

    async def _update_page_layout(self, page_id: int, column_type: str):
        type_settings = self._get_type_settings(column_type)
        update = {
            "groupId": str(self.config.site_id),
            "privateLayout": "false",
            "layoutId": page_id,
            "typeSettings": type_settings
        }
        
        async with self.session.post(
            f"{self.config.liferay_url}/api/jsonws/layout/update-layout",
            params=update
        ) as update_response:
            return update_response.status in (200, 201)

    def _get_type_settings(self, column_type: str) -> str:
        random_id = random.randint(10000, 99999)  # Gera um número aleatório de 5 dígitos
        settings = {
            "1_column": (
                f"column-1=com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_{random_id}\n"
                f"layout-template-id={column_type}\n"
            ),
            "2_columns_ii": (
                f"column-1=com_liferay_site_navigation_menu_web_portlet_SiteNavigationMenuPortlet_INSTANCE_{random_id}\n"
                f"column-2=com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_{random_id}\n"
                f"layout-template-id={column_type}\n"
            )
        }
        return settings.get(column_type, "")

    async def ensure_page_exists(self, title: str, cache_key: str, parent_id: int = 0, 
                               friendly_url: str = "", hierarchy: List[str] = None, 
                               page_type: str = "portlet", visible: bool = True, 
                               column_type: str = "1_column") -> int:
        if cache_key in self.page_cache:
            return self.page_cache[cache_key]

        page_id = await self.create_page(title, friendly_url, parent_id, 
                                       hierarchy, page_type, visible, column_type)
        
        if page_id:
            self.page_cache[cache_key] = page_id
            
        return page_id

    async def create_hierarchy(self, hierarchy: list, final_title: str, final_url: str, 
                             page_type: str = "widget", visible: bool = True, 
                             column_type: str = "1_column") -> int:
        current_path = ""
        parent_id = 0
        last_page_id = 0

        hierarchy_levels = [x for x in hierarchy if x.lower() != 'raiz']

        for level in hierarchy_levels:
            normalized_level = self.processor.normalize_page_name(level)
            current_path += f" > {normalized_level}" if current_path else normalized_level
            level_id = await self.ensure_page_exists(normalized_level, current_path, 
                                                   parent_id, final_url, hierarchy, 
                                                   page_type, visible, column_type)
            
            if level_id:
                parent_id = level_id
                last_page_id = level_id
            else:
                print(f"Falha ao criar nível: {normalized_level}")
                return 0

        if self._should_create_final_page(hierarchy_levels, final_title):
            final_page_id = await self.create_page(final_title, final_url, parent_id, 
                                                 hierarchy, page_type, visible, column_type)
            print(f"Página final criada: {final_title} (ID: {final_page_id}) Tipo da página {page_type}")
            
            if final_page_id:
                last_page_id = final_page_id

        return last_page_id

    def _should_create_final_page(self, hierarchy_levels: List[str], final_title: str) -> bool:
        return (not hierarchy_levels or 
                self.processor.normalize_page_name(final_title).lower() != 
                self.processor.normalize_page_name(hierarchy_levels[-1]).lower())
    
    async def retry_failed_pages(self):
        failed_pages = self.error_tracker.get_failed_pages()
        if not failed_pages:
            return
        
        for error in failed_pages:
            if error.retry_count < 3:
                error.retry_count += 1
                await self.create_page(error.title, error.url, error.parent_id, error.hierarchy)

        failed_pages = self.error_tracker.get_failed_pages()
        if not failed_pages:
            return
        
        for error in failed_pages:
            if error.retry_count < 3:
                error.retry_count += 1
                await self.create_page(error.title, error.url, error.parent_id, error.hierarchy)