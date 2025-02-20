import random
from typing import List, Optional
from errors.page_error import ErrorTracker, PageError
from processors.page_processor import PageProcessor

class PageCreator:
    def __init__(self, config):
        self.config = config
        self.session = None
        self.page_cache = {}
        self.error_tracker = ErrorTracker()
        self.processor = PageProcessor()

    async def create_page(self, title: str, friendly_url: str, parent_id: int = 0, 
                         hierarchy: List[str] = None, page_type: str = "portlet", 
                         visible: bool = True, column_type: str = "1_column",
                         menu_title: str = None, url_vinculada: str = "") -> int:
        try:
            normalized_title = self.processor.normalize_page_name(title)
            normalized_url = self.processor.normalize_friendly_url(friendly_url)
            page_id = await self._create_page_request(normalized_title, normalized_url, 
                                                    parent_id, visible, page_type)
            print(f"url_vinculada 2 {url_vinculada}")
            
            if page_id:
                await self._update_page_layout(page_id, column_type, hierarchy, menu_title, url_vinculada)
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

    async def _update_page_layout(self, page_id: int, column_type: str, 
                               hierarchy: List[str] = None, menu_title: str = None, url_vinculada: str= ""):
        type_settings = self._get_type_settings(column_type, url_vinculada)
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
            success = update_response.status in (200, 201)
            
            # Se a atualização foi bem-sucedida e é um layout com menu, configura o menu
            if success and column_type == "2_columns_ii":
                menu_portlet_id = self._extract_menu_portlet_id(type_settings)
                if menu_portlet_id and hasattr(self.config, 'display_template_key'):
                    menu_level = self._determine_menu_level_from_hierarchy(hierarchy)
                    
                    await self.configure_menu_display(
                        page_id, 
                        menu_portlet_id, 
                        self.config.display_template_key,
                        menu_title,
                        menu_level
                    )
            
            return success

    def _extract_menu_title_from_hierarchy(self, hierarchy: List[str] = None) -> str:
        """
        Extrai um título de menu baseado na hierarquia como fallback
        quando não há título fornecido pela planilha
        """
        if not hierarchy or len(hierarchy) == 0:
            return "Menu de Navegação"
            
        # Remove 'Raiz' da hierarquia se existir
        clean_hierarchy = [x for x in hierarchy if x.lower() != 'raiz']
        
        # Se a hierarquia estiver vazia após limpeza, retorna título padrão
        if not clean_hierarchy:
            return "Menu de Navegação"
        
        # Se tivermos uma hierarquia completa, o título do menu deve ser o último elemento
        # ou o penúltimo se a hierarquia for profunda
        if len(clean_hierarchy) >= 3:
            # Para hierarquias mais profundas, usamos o nível anterior como título do menu
            menu_title = self.processor.normalize_page_name(clean_hierarchy[-2])
        else:
            # Para hierarquias mais rasas, usamos o último elemento como título
            menu_title = self.processor.normalize_page_name(clean_hierarchy[-1])
            
        return menu_title

    def _determine_menu_level_from_hierarchy(self, hierarchy: List[str] = None) -> int:
        """
        Determina o nível do menu baseado na hierarquia
        """
        if not hierarchy:
            return 0
            
        clean_hierarchy = [x for x in hierarchy if x.lower() != 'raiz']
        
        # Limita ao máximo de 4 níveis
        hierarchy_depth = min(len(clean_hierarchy), 4)
        
        # Para hierarquias mais profundas (3+), mostramos a partir do nível 1
        if hierarchy_depth > 2:
            return 1
        
        return 0

    def _get_type_settings(self, column_type: str, url_vinculada: str) -> str:
        random_id = random.randint(10000, 99999)  # Gera um número aleatório de 5 dígitos
        settings = {
            "1_column": (
                f"column-1=com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_{random_id}\nlayoutUpdateable=true\nurl={url_vinculada}\n"
                f"layout-template-id={column_type}\n"
            ),
            "2_columns_ii": (
                f"column-1=com_liferay_site_navigation_menu_web_portlet_SiteNavigationMenuPortlet_INSTANCE_{random_id}\n"
                f"column-2=com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_{random_id}\n"
                f"layout-template-id={column_type}\n"
            )
        }
        return settings.get(column_type, "")
    
    def _extract_menu_portlet_id(self, type_settings: str) -> str:
        """
        Extrai o ID do portlet de menu das configurações de tipo
        
        Args:
            type_settings: String de configurações retornada por _get_type_settings
            
        Returns:
            str: ID do portlet ou None se não encontrado
        """
        if "SiteNavigationMenuPortlet" not in type_settings:
            return None
            
        for line in type_settings.split("\n"):
            if "SiteNavigationMenuPortlet" in line:
                parts = line.split("=")
                if len(parts) > 1:
                    return parts[1].strip()
        return None
    
    async def configure_menu_display(self, page_id: int, portlet_id: str, 
                                   display_template_key: str, menu_title: str = None,
                                   root_menu_item_level: int = None) -> bool:
        """
        Configura o template de exibição para o portlet de menu de navegação
        
        Args:
            page_id: ID da página
            portlet_id: ID do portlet de menu (SiteNavigationMenuPortlet)
            display_template_key: Chave do template de exibição (do env)
            menu_title: Título do menu (extraído da planilha ou calculado da hierarquia)
            root_menu_item_level: Nível de navegação do menu (calculado da hierarquia)
            
        Returns:
            bool: True se configurado com sucesso, False caso contrário
        """
        try:
            if menu_title is None:
                menu_title = "Menu de Navegação"
                
            if root_menu_item_level is None:
                root_menu_item_level = 0
                
            params = {
                "plid": str(page_id),
                "portletId": portlet_id,
                "displayTemplateKey": display_template_key,
                "rootMenuItemLevel": str(root_menu_item_level),
                "portletSetupTitle": menu_title
            }
            
            async with self.session.post(
                f"{self.config.liferay_url}/o/api-association-migrador/v1.0/site-navigation/associate-menu",
                params=params
            ) as response:
                success = response.status in (200, 201)
                if success:
                    print(f"Menu configurado com sucesso para página {page_id} com template {display_template_key}")
                    print(f"Nível do menu: {root_menu_item_level}, Título: {menu_title}")
                else:
                    error_text = await response.text()
                    print(f"Erro ao configurar menu: {response.status} - {error_text}")
                return success
        except Exception as e:
            print(f"Exceção ao configurar menu: {str(e)}")
            return False

    async def ensure_page_exists(self, title: str, cache_key: str, parent_id: int = 0, 
                               friendly_url: str = "", hierarchy: List[str] = None, 
                               page_type: str = "portlet", visible: bool = True, 
                               column_type: str = "1_column", menu_title: str = None, url_vinculada: str= None) -> int:
        """
        Verifica se uma página já existe no cache ou cria uma nova
        
        Args:
            title: Título da página
            cache_key: Chave para o cache de páginas
            parent_id: ID da página pai
            friendly_url: URL amigável
            hierarchy: Lista da hierarquia completa até este nível
            page_type: Tipo de página ('portlet', 'widget', etc)
            visible: Se a página é visível
            column_type: Tipo de layout de coluna
            menu_title: Título do menu lateral (opcional)
            
        Returns:
            int: ID da página encontrada ou criada
        """
        if cache_key in self.page_cache:
            return self.page_cache[cache_key]

        # Cria a URL amigável se não fornecida
        if not friendly_url:
            friendly_url = self.processor.normalize_friendly_url(title)
        print(f"url_vinculada {url_vinculada}")
        
        page_id = await self.create_page(title, friendly_url, parent_id, 
                                       hierarchy, page_type, visible, column_type,
                                       menu_title, url_vinculada)
        
        if page_id:
            self.page_cache[cache_key] = page_id
            
        return page_id

    async def create_hierarchy(self, hierarchy: list, final_title: str, final_url: str, 
                             page_type: str = "widget", visible: bool = True, 
                             column_type: str = "1_column", menu_title: str = None, url_vinculada: str= None) -> int:
        current_path = ""
        parent_id = 0
        last_page_id = 0

        hierarchy_levels = [x for x in hierarchy if x.lower() != 'raiz']

        for level in hierarchy_levels:
            normalized_level = self.processor.normalize_page_name(level)
            current_path += f" > {normalized_level}" if current_path else normalized_level
            level_id = await self.ensure_page_exists(
                normalized_level, current_path, parent_id, "", 
                hierarchy_levels[:hierarchy_levels.index(level)+1], 
                page_type, visible, column_type, menu_title, url_vinculada
            )
            
            if level_id:
                parent_id = level_id
                last_page_id = level_id
            else:
                print(f"Falha ao criar nível: {normalized_level}")
                return 0

        if self._should_create_final_page(hierarchy_levels, final_title):
            final_hierarchy = hierarchy_levels.copy()
            if final_title not in final_hierarchy:
                final_hierarchy.append(final_title)
                
            final_page_id = await self.create_page(
                final_title, final_url, parent_id, final_hierarchy,
                page_type, visible, column_type, menu_title, url_vinculada
            )
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