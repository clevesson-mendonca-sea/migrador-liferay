import os
import traceback
from bs4 import BeautifulSoup
import json
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class CollapseContentProcessor:
    def __init__(self, config):
        self.config = config
        self.structure_id = os.getenv('LIFERAY_COLAPSE_STRUCTURE_ID', '29541')
        logger.info(f"Initialized CollapseContentProcessor with structure ID: {self.structure_id}")
        
        
    def process_collapse_content(self, html_content: str) -> Dict[str, Any]:
        """
        Processa o conteúdo HTML e extrai os elementos colapsáveis
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        panels = soup.find_all('div', class_='panel-default')
        
        # Lista que conterá todos os grupos de campos
        content_fields = []
        
        for panel in panels:
            # Extrai o título do painel
            panel_heading = panel.find('div', class_='panel-heading')
            if not panel_heading:
                continue
                
            panel_title = panel_heading.find('h4', class_='panel-title')
            title_text = panel_title.get_text(strip=True) if panel_title else ""
            
            # Determina a cor baseada na classe
            panel_color = "azul"  # Azul por padrão
            if "verde" in (panel_heading.get('class', []) or []):
                panel_color = "verde"
            
            # Extrai o conteúdo do painel
            panel_body = panel.find('div', class_='panel-body')
            if not panel_body:
                continue
                
            # Remove atributos indesejados e scripts
            for tag in panel_body.find_all(True):
                attrs = dict(tag.attrs)
                for attr in attrs:
                    if attr.startswith('data-') or attr.startswith('aria-') or attr in ['role', 'style']:
                        del tag[attr]
            
            # Converte o conteúdo para string mantendo a formatação
            content_html = str(panel_body)

            # Cria o grupo de campos seguindo a estrutura DDM
            group_field = {
                "name": "GrupoDeCampos29760553",
                "contentFieldValue": {
                    "data": [
                        {
                            "collapse_title": title_text,
                            "collapse_color": panel_color,
                            "collapse_content": content_html
                        }
                    ]
                }
            }
            
            content_fields.append(group_field)
        
        # Monta a estrutura final do conteúdo
        content_data = {
            "contentStructureId": self.structure_id,
            "structuredContentFolderId": None,  # Será definido no create_collapse_content
            # Removido o campo title daqui pois será definido no create_collapse_content
            "contentFields": content_fields
        }
        
        return content_data

    async def create_collapse_content(self, web_content_creator, title: str, html_content: str, folder_id: int) -> int:
        """
        Cria o conteúdo colapsável no Liferay
        """
        if not web_content_creator.session:
            await web_content_creator.initialize_session()
            
        try:
            # Processa o conteúdo HTML
            content_data = self.process_collapse_content(html_content)
            content_data["structuredContentFolderId"] = folder_id
            
            # Ajusta o formato do título
            content_data["title"] = {"pt_BR": title}  # Ajustado para incluir a localização
            
            url = f"{web_content_creator.config.liferay_url}/o/headless-delivery/v1.0/structured-content-folders/{folder_id}/structured-contents"
            
            # Log da estrutura sendo enviada
            logger.debug(f"Sending collapse content structure: {json.dumps(content_data, indent=2)}")
            
            async with web_content_creator.session.post(url, json=content_data) as response:
                response_text = await response.text()
                if response.status in (200, 201):
                    result = json.loads(response_text)
                    content_id = result.get('id')
                    if content_id:
                        logger.info(f"Created collapse content: {title} (ID: {content_id})")
                        return int(content_id)
                
                logger.error(f"Failed to create collapse content: {title}")
                logger.error(f"Response: {response_text}")
                
        except Exception as e:
            logger.error(f"Error creating collapse content {title}: {str(e)}")
            logger.error(traceback.format_exc())
        
        return 0