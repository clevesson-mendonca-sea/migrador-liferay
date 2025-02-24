import os
import traceback
from bs4 import BeautifulSoup
import json
import logging
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

class CollapseContentProcessor:
    def __init__(self, config):
        self.config = config
        self.structure_id = os.getenv('LIFERAY_COLAPSE_STRUCTURE_ID', '')
        
    def _get_panel_color(self, panel_heading):
        """Determina a cor do painel"""
        try:
            style = panel_heading.get('style', '').lower()
            if 'background' in style:
                if 'gray' in style or 'grey' in style:
                    return "Cinza"
                elif 'green' in style or '#eaf2e9;' in style:
                    return "Verde"
                elif 'blue' in style or 'azul' in style:
                    return "Azul"
            return "Azul"
        except Exception as e:
            logger.error(f"Error determining panel color: {str(e)}")
            return "Azul"

    def extract_nested_panels(self, panel_body) -> List[Dict]:
        """Extrai painéis aninhados do corpo do painel principal"""
        nested_panels = []
        nested_elements = panel_body.find_all('div', class_=['panel panel-success', 'panel panel-default', 'panel'])
        
        # Remove os painéis aninhados do corpo original para não duplicar
        for nested_panel in nested_elements:
            nested_panel.extract()
            nested_panels.append(nested_panel)
            
        return nested_panels

    async def process_panel(self, web_content_creator, panel, folder_id: int, title_prefix: str = "") -> Tuple[Optional[Dict], List[int]]:
        """Processa um painel individual e seus filhos"""
        try:
            # Extrai os dados básicos do painel
            panel_heading = panel.find('div', class_='panel-heading')
            if not panel_heading:
                return None, []

            panel_color = self._get_panel_color(panel_heading)
            
            # Busca título
            panel_title = panel_heading.find('p', class_='panel-title')
            if not panel_title:
                panel_title = panel_heading.find(['h3', 'h4', 'p'])
            
            title_text = panel_title.get_text(strip=True) if panel_title else "Seção"
            title_text = title_text.replace('⇵', '').strip()
            
            # Prepara o título completo com prefixo se necessário
            full_title = f"{title_prefix} - {title_text}" if title_prefix else title_text

            # Busca o corpo e extrai painéis aninhados
            panel_collapse = panel.find('div', class_='panel-collapse')
            panel_body = (panel_collapse and panel_collapse.find('div', class_='panel-body')) or panel.find('div', class_='panel-body')
            
            if not panel_body:
                return None, []

            # Extrai e processa painéis aninhados
            nested_panels = self.extract_nested_panels(panel_body)
            nested_content_ids = []
            
            # Cria os painéis aninhados primeiro
            for nested_panel in nested_panels:
                nested_result = await self.process_panel(
                    web_content_creator, 
                    nested_panel,
                    folder_id,
                    full_title
                )
                if nested_result and nested_result[0]:
                    nested_content_ids.extend(nested_result[1])

            # Limpa o HTML do corpo
            for tag in panel_body.find_all(True):
                attrs = dict(tag.attrs)
                allowed_attrs = {'src', 'href', 'style', 'class'}
                for attr in attrs:
                    if attr not in allowed_attrs:
                        del tag[attr]

            content_html = str(panel_body)

            # Cria o conteúdo principal
            content_fields = [{
                "name": "collapse",
                "nestedContentFields": [
                    {
                        "name": "collapse_title",
                        "contentFieldValue": {
                            "data": title_text
                        }
                    },
                    {
                        "name": "collapse_collor",
                        "contentFieldValue": {
                            "data": panel_color,
                            "value": panel_color.lower()
                        }
                    },
                    {
                        "name": "collapse_content",
                        "contentFieldValue": {
                            "data": content_html
                        }
                    }
                ]
            }]

            # Adiciona referências aos painéis aninhados se houver
            if nested_content_ids:
                for content_id in nested_content_ids:
                    content_fields[0]["nestedContentFields"].append({
                        "name": "groupCollapse",
                        "contentFieldValue": {
                            "data": str(content_id)
                        }
                    })

            # Cria o conteúdo no Liferay
            content_data = {
                "contentStructureId": self.structure_id,
                "contentFields": content_fields,
                "structuredContentFolderId": folder_id,
                "title": full_title,
                "friendlyUrlPath": web_content_creator.url_utils.sanitize_content_path(full_title)
            }

            content_id = await self.create_content(web_content_creator, content_data)
            if content_id:
                return content_data, [content_id] + nested_content_ids
            
            return None, nested_content_ids

        except Exception as e:
            logger.error(f"Error processing panel: {str(e)}")
            return None, []

    async def create_content(self, web_content_creator, content_data: Dict) -> Optional[int]:
        """Cria o conteúdo no Liferay"""
        if not web_content_creator.session:
            await web_content_creator.initialize_session()

        try:
            url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{content_data['structuredContentFolderId']}/structured-contents"

            async def create_attempt():
                async with web_content_creator.session.post(url, json=content_data) as response:
                    response_text = await response.text()
                    
                    if response.status in (200, 201):
                        result = json.loads(response_text)
                        content_id = result.get('id')
                        if content_id:
                            logger.info(f"Created collapse content: {content_data['title']} (ID: {content_id})")
                            return int(content_id)
                    
                    raise Exception(f"Content creation failed: {response.status}")

            return await web_content_creator._retry_operation(create_attempt)

        except Exception as e:
            logger.error(f"Error creating content: {str(e)}")
            return None

    async def create_collapse_content(self, web_content_creator, title: str, html_content: str, folder_id: int):
        """Ponto de entrada principal para criar conteúdo colapsável"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            root_panels = soup.find_all('div', class_=['panel panel-success', 'panel panel-default', 'panel'], recursive=False)
            
            logger.info(f"Found {len(root_panels)} root panels to process")
            
            all_content_ids = []
            for panel in root_panels:
                result = await self.process_panel(web_content_creator, panel, folder_id)
                if result and result[1]:
                    all_content_ids.extend(result[1])
            
            # Retorna o primeiro ID que será o conteúdo principal
            return all_content_ids[0] if all_content_ids else 0

        except Exception as e:
            logger.error(f"Error in create_collapse_content: {str(e)}")
            logger.error(traceback.format_exc())
            return 0