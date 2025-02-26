import logging
from bs4 import BeautifulSoup
import json
import os

class TabContentProcessor:
    def __init__(self, config):
        """
        Inicializa o processador de conteúdo de abas
        
        :param config: Configuração com detalhes de conexão e estrutura
        """
        self.config = config
        self.tab_structure_id = os.getenv('LIFERAY_TAB_STRUCTURE_ID', '')
        self.logger = logging.getLogger(__name__)

    def _extract_tab_data(self, html_content: str):
        """
        Extrai os dados das abas a partir do HTML
        
        :param html_content: Conteúdo HTML com abas
        :return: Lista de dicionários com informações das abas
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Encontra a lista de abas
        tab_list = soup.find('ul', class_='nav-tabs')
        if not tab_list:
            self.logger.warning("No tab list found in the HTML")
            return []
        
        # Encontra o conteúdo das abas
        tab_content = soup.find('div', class_='tab-content')
        if not tab_content:
            self.logger.warning("No tab content found in the HTML")
            return []
        
        tabs = []
        for tab_item in tab_list.find_all('li'):
            # Extrai informações da aba
            tab_link = tab_item.find('a')
            if not tab_link:
                continue
            
            # Extrai título da aba
            tab_title = tab_link.get_text(strip=True)
            
            # Extrai identificador da aba
            tab_href = tab_link.get('href', '').strip('#')
            
            # Encontra o conteúdo correspondente
            tab_pane = tab_content.find('div', id=tab_href)
            if not tab_pane:
                continue
            
            # Extrai cor da aba
            tab_color = tab_link.get('style', '').replace('background-color: ', '').replace(';', '').strip()
            
            # Limpa conteúdo removendo atributos desnecessários
            for tag in tab_pane.find_all(True):
                attrs = dict(tag.attrs)
                allowed_attrs = {'src', 'href', 'style', 'class'}
                for attr in attrs:
                    if attr not in allowed_attrs:
                        del tag[attr]
            
            # Converte conteúdo para string
            content_html = str(tab_pane)
            
            tabs.append({
                'title': tab_title,
                'color': tab_color,
                'content': content_html
            })
        
        return tabs

    def process_tab_content(self, html_content: str):
        """
        Processa o conteúdo das abas e gera uma estrutura para o Liferay
        
        :param html_content: Conteúdo HTML com abas
        :return: Lista de campos de conteúdo para estrutura de abas
        """
        tabs_data = self._extract_tab_data(html_content)
        if not tabs_data:
            self.logger.error("No tabs found to process")
            return []
        
        content_fields = []
        for tab in tabs_data:
            tab_field = {
                "name": "Tab",
                "nestedContentFields": [
                    {
                        "name": "tab_title",
                        "contentFieldValue": {
                            "data": tab['title']
                        }
                    },
                    {
                        "name": "tab_color",
                        "contentFieldValue": {
                            "data": tab['color'],
                            "value": tab['color']
                        }
                    },
                    {
                        "name": "tab_btn_color",
                        "contentFieldValue": {
                            "data": tab['color'],
                            "value": tab['color']
                        }
                    },
                    {
                        "name": "content",
                        "contentFieldValue": {
                            "data": tab['content']
                        }
                    }
                ]
            }
            content_fields.append(tab_field)
        
        return content_fields

    async def create_tab_content(self, web_content_creator, title: str, html_content: str, folder_id: int):
        """
        Cria conteúdo de abas no Liferay
        
        :param web_content_creator: Instância do criador de conteúdo web
        :param title: Título do conteúdo
        :param html_content: Conteúdo HTML com abas
        :param folder_id: ID da pasta onde o conteúdo será criado
        :return: Resultado da criação do conteúdo
        """
        if not web_content_creator.session:
            await web_content_creator.initialize_session()

        try:
            # Processa o conteúdo das abas
            content_fields = self.process_tab_content(html_content)
            if not content_fields:
                raise Exception("No valid content fields were generated")

            # Limitar título se necessário
            if len(title) > 255:
                truncated_title = title[:250] + "..."
                self.logger.info(f"Título truncado para criação de conteúdo: '{title}' -> '{truncated_title}'")
                title = truncated_title

            # Monta o payload com a estrutura exata esperada
            content_data = {
                "contentStructureId": self.tab_structure_id,
                "contentFields": content_fields,
                "structuredContentFolderId": folder_id,
                "title": title,
                "friendlyUrlPath": web_content_creator.url_utils.sanitize_content_path(title)
            }

            self.logger.debug(f"Sending tab content data: {json.dumps(content_data, indent=2)}")

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
                                self.logger.info(f"Successfully created tab content: {title} (ID: {content_id})")
                                return int(content_key)
                        except json.JSONDecodeError as je:
                            self.logger.error(f"Failed to parse response JSON: {str(je)}")
                            raise Exception(f"Invalid JSON response: {response_text}")
                    
                    self.logger.error(f"Failed to create tab content. Status: {response.status}")
                    raise Exception(f"Content creation failed with status {response.status}: {response_text}")

            return await web_content_creator._retry_operation(create_attempt)
        except Exception as e:
            self.logger.error(f"Error creating tab content: {str(e)}")
            return False