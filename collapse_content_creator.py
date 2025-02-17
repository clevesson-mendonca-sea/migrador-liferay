import asyncio
import os
import traceback
from bs4 import BeautifulSoup
import json
import logging

logger = logging.getLogger(__name__)

class CollapseContentProcessor:
    def __init__(self, config):
        self.config = config
        self.structure_id = os.getenv('LIFERAY_COLAPSE_STRUCTURE_ID', '')
        
class CollapseContentProcessor:
    def __init__(self, config):
        self.config = config
        self.structure_id = os.getenv('LIFERAY_COLAPSE_STRUCTURE_ID', '')
        
    def _get_panel_color(self, panel_heading):
        """
        Determina a cor do painel baseado nas classes e estilos.
        Prioriza o background do style.
        """
        try:
            # Verifica o estilo background
            style = panel_heading.get('style', '').lower()
            if 'background' in style:
                if 'gray' in style or 'grey' in style:
                    return "Cinza"
                elif 'green' in style or 'verde' in style:
                    return "Verde"
                elif 'blue' in style or 'azul' in style:
                    return "Azul"
            
            # # Se não encontrou no background, verifica as classes
            # classes = ' '.join(panel_heading.get('class', [])).lower()
            # if 'verde' in classes or 'success' in classes:
            #     return "Verde"
            # elif 'vermelho' in classes or 'danger' in classes:
            #     return "Vermelho"
            # elif 'cinza' in classes or 'gray' in classes:
            #     return "Cinza"
            
            # Cor padrão se nenhuma for encontrada
            return "Azul"
        except Exception as e:
            logger.error(f"Error determining panel color: {str(e)}")
            return "Azul"

    def process_collapse_content(self, html_content: str):
        """
        Processa o HTML e extrai os elementos colapsáveis seguindo o padrão específico do site.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        panels = soup.find_all('div', class_=['panel panel-success', 'panel panel-default', 'panel'])
        
        logger.info(f"Found {len(panels)} panels to process")

        def extract_panel_data(panel):
            """ Extrai os dados de um painel seguindo o padrão específico do site. """
            logger.debug("Extracting panel data")
            
            # Busca cabeçalho
            panel_heading = panel.find('div', class_='panel-heading')
            if not panel_heading:
                logger.debug("No panel-heading found")
                return None

            # Determina a cor usando o novo método
            panel_color = self._get_panel_color(panel_heading)
            logger.debug(f"Determined panel color: {panel_color}")

            # Busca título dentro do panel-heading
            panel_title = panel_heading.find('p', class_='panel-title')
            if not panel_title:
                panel_title = panel_heading.find(['h3', 'h4', 'p'])
            
            # Extrai o texto do título, removendo a seta (⇵) se presente
            title_text = panel_title.get_text(strip=True) if panel_title else ""
            title_text = title_text.replace('⇵', '').strip()
            if not title_text:
                title_text = "Seção"
            
            logger.debug(f"Found title: {title_text}")

            # Busca o corpo do painel na estrutura específica
            panel_collapse = panel.find('div', class_='panel-collapse')
            panel_body = (panel_collapse and panel_collapse.find('div', class_='panel-body')) or panel.find('div', class_='panel-body')
            
            if not panel_body:
                logger.debug("No panel-body found")
                return None

            # Remove atributos desnecessários mantendo apenas o conteúdo
            for tag in panel_body.find_all(True):
                attrs = dict(tag.attrs)
                allowed_attrs = {'src', 'href', 'style', 'class'}
                for attr in attrs:
                    if attr not in allowed_attrs:
                        del tag[attr]

            content_html = str(panel_body)

            # Estrutura do campo colapsável
            collapse_field = {
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
            }

            return collapse_field

        content_fields = []
        for panel in panels:
            panel_data = extract_panel_data(panel)
            if panel_data:
                content_fields.append(panel_data)
                logger.debug(f"Added panel data: {json.dumps(panel_data, indent=2)}")

        if not content_fields:
            logger.error("No valid content fields were generated")
            return []

        logger.info(f"Successfully processed {len(content_fields)} panels")
        return content_fields

    async def create_collapse_content(self, web_content_creator, title: str, html_content: str, folder_id: int):
        """Cria um conteúdo colapsável no Liferay mantendo a estrutura original"""
        if not web_content_creator.session:
            await web_content_creator.initialize_session()

        try:
            content_fields = self.process_collapse_content(html_content)
            if not content_fields:
                raise Exception("No valid content fields were generated")

            # Monta o payload com a estrutura exata esperada
            content_data = {
                "contentStructureId": self.structure_id,
                "contentFields": content_fields,
                "structuredContentFolderId": folder_id,
                "title": title,
                "friendlyUrlPath": web_content_creator.url_utils.sanitize_content_path(title)
            }

            logger.debug(f"Sending content data: {json.dumps(content_data, indent=2)}")

            url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{folder_id}/structured-contents"

            async def create_attempt():
                async with web_content_creator.session.post(url, json=content_data) as response:
                    response_text = await response.text()
                    
                    if response.status in (200, 201):
                        try:
                            result = json.loads(response_text)
                            content_id = result.get('id')
                            if content_id:
                                logger.info(f"Successfully created collapse content: {title} (ID: {content_id})")
                                return int(content_id)
                        except json.JSONDecodeError as je:
                            logger.error(f"Failed to parse response JSON: {str(je)}")
                            raise Exception(f"Invalid JSON response: {response_text}")
                    
                    logger.error(f"Failed to create collapse content. Status: {response.status}")
                    logger.error(f"Response: {response_text}")
                    logger.error(f"Request data: {json.dumps(content_data, indent=2)}")
                    raise Exception(f"Content creation failed with status {response.status}: {response_text}")

            return await web_content_creator._retry_operation(create_attempt)

        except Exception as e:
            logger.error(f"Error creating collapse content '{title}': {str(e)}")
            logger.error(traceback.format_exc())
            web_content_creator._log_error("Collapse Content Creation", title, str(e))
            return 0