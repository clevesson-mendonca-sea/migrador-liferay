import logging
import traceback
from bs4 import BeautifulSoup
import json
import os

logger = logging.getLogger(__name__)

class CollapseContentProcessor:
    def __init__(self, config):
        self.config = config
        self.structure_id = os.getenv('LIFERAY_COLAPSE_STRUCTURE_ID', '')
        
    def _get_panel_color(self, element):
        """
        Determina a cor do painel baseado nas classes e estilos.
        Funciona tanto para panel-heading quanto para botões.
        """
        try:
            # Verifica se o parent tem panel-success
            parent = element.parent
            if parent and hasattr(parent, 'get') and parent.get('class'):
                parent_classes = ' '.join(parent.get('class', [])).lower() if isinstance(parent.get('class'), list) else parent.get('class', '').lower()
                if 'panel-success' in parent_classes:
                    return "Verde"
            
            # Verifica se o próprio elemento tem panel-success
            element_classes = ' '.join(element.get('class', [])).lower() if isinstance(element.get('class'), list) else element.get('class', '').lower()
            if 'panel-success' in element_classes:
                return "Verde"
                
            # Verifica o estilo background
            style = element.get('style', '').lower()
            if 'background' in style:
                if 'gray' in style or 'grey' in style:
                    return "Cinza"
                elif 'green' in style or '#eaf2e9;' in style:
                    return "Verde"
                elif 'blue' in style or 'azul' in style:
                    return "Azul"
            
            # Se é um botão, verifica as classes do botão
            classes = ' '.join(element.get('class', [])).lower() if isinstance(element.get('class'), list) else element.get('class', '').lower()
            
            # Botões bootstrap
            if 'btn-primary' in classes:
                return "Azul"
            elif 'btn-success' in classes:
                return "Verde"
            elif 'btn-default' in classes or 'btn-secondary' in classes:
                return "Cinza"
            elif 'btn-danger' in classes or 'btn-warning' in classes:
                return "Vermelho"
            
            return "Azul"
        except Exception as e:
            logger.error(f"Error determining panel color: {str(e)}")
            return "Azul"

    def _extract_panel_data(self, panel):
        """Extrai os dados de um painel tradicional"""
        logger.debug("Extracting panel data")
        
        # Busca cabeçalho
        panel_heading = panel.find('div', class_='panel-heading')
        if not panel_heading:
            logger.debug("No panel-heading found")
            return None

        # Determina a cor usando o método
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
        return self._create_collapse_field(title_text, panel_color, content_html)

    def _extract_button_collapse_data(self, button, collapse_div):
        """Extrai dados de colapsáveis baseados em botão"""
        logger.debug("Extracting button collapse data")
        
        # Obter título do botão
        title_text = button.get_text(strip=True)
        title_text = title_text.replace('⇵', '').strip()
        if not title_text:
            title_text = "Seção"
        
        logger.debug(f"Found button title: {title_text}")
        
        # Determinar cor do botão
        button_color = self._get_panel_color(button)
        logger.debug(f"Determined button color: {button_color}")
        
        # Encontrar conteúdo dentro do collapse
        content_container = collapse_div.find('div', class_='well')
        if not content_container:
            # Se não encontrar div.well, usa o próprio collapse_div como conteúdo
            content_container = collapse_div
        
        # Limpar atributos desnecessários
        for tag in content_container.find_all(True):
            attrs = dict(tag.attrs)
            allowed_attrs = {'src', 'href', 'style', 'class'}
            for attr in attrs:
                if attr not in allowed_attrs:
                    del tag[attr]
        
        content_html = str(content_container)
        
        # Criar campo colapsável
        return self._create_collapse_field(title_text, button_color, content_html)

    def _create_collapse_field(self, title, color, content):
        """Cria estrutura de campo colapsável padronizada"""
        return {
            "name": "collapse",
            "nestedContentFields": [
                {
                    "name": "collapse_title",
                    "contentFieldValue": {
                        "data": title
                    }
                },
                {
                    "name": "collapse_collor",
                    "contentFieldValue": {
                        "data": color,
                        "value": color.lower()
                    }
                },
                {
                    "name": "collapse_content",
                    "contentFieldValue": {
                        "data": content
                    }
                }
            ]
        }

    def process_collapse_content(self, html_content: str):
        """
        Processa o HTML e extrai os elementos colapsáveis de diferentes tipos.
        Suporta tanto painéis tradicionais quanto botões com collapse.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        content_fields = []
        
        # 1. Processar painéis tradicionais
        panels = soup.find_all('div', class_=['panel panel-success', 'panel panel-default', 'panel'])
        logger.info(f"Found {len(panels)} traditional panels to process")
        
        for panel in panels:
            panel_data = self._extract_panel_data(panel)
            if panel_data:
                content_fields.append(panel_data)
        
        # 2. Processar botões com collapse
        buttons = soup.select('button[data-toggle="collapse"]')
        logger.info(f"Found {len(buttons)} collapse buttons to process")
        
        for button in buttons:
            # Obter ID do collapse alvo
            target_id = button.get('data-target', '').strip('#')
            if not target_id:
                continue
                
            # Encontrar div de collapse correspondente
            collapse_div = soup.find('div', id=target_id)
            if not collapse_div:
                continue
                
            button_data = self._extract_button_collapse_data(button, collapse_div)
            if button_data:
                content_fields.append(button_data)
        
        if not content_fields:
            logger.error("No valid content fields were generated")
            return []

        logger.info(f"Successfully processed total of {len(content_fields)} collapsible elements")
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
                            content_key = result.get('key')
                            
                            if content_id:
                                logger.info(f"Successfully created collapse content: {title} (ID: {content_id})")
                                return int(content_key)
                        except json.JSONDecodeError as je:
                            logger.error(f"Failed to parse response JSON: {str(je)}")
                            raise Exception(f"Invalid JSON response: {response_text}")
                    
                    logger.error(f"Failed to create collapse content. Status: {response.status}")
                    raise Exception(f"Content creation failed with status {response.status}: {response_text}")

            return await web_content_creator._retry_operation(create_attempt)
        except Exception as e:
            logger.error(f"Error creating collapsible content: {str(e)}")
            return False