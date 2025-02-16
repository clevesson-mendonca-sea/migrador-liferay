import traceback
from typing import Dict, List
from bs4 import BeautifulSoup
import logging
import re
from collapse_content_creator import CollapseContentProcessor


class MixedContentProcessor:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.collapse_processor = CollapseContentProcessor(config)

    def split_content(self, html_content: str) -> List[Dict[str, str]]:
        """
        Splits mixed content into regular content and one collapsible section.
        Returns a list of dictionaries with content type and HTML.
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            sections = []
            
            # Encontra o container principal
            main_content = soup.find('div', id='conteudo')
            if not main_content:
                main_content = soup.find('div', class_=['corpo-principal', 'col-md-8'])
            if not main_content:
                main_content = soup
                
            # Encontra todos os panels no conteúdo
            all_panels = main_content.find_all('div', class_='panel-default')
            
            if not all_panels:
                # Se não tem panels, retorna como conteúdo regular
                return [{
                    'type': 'regular',
                    'content': str(main_content)
                }]
                
            # Trabalha com uma cópia do conteúdo para não modificar o original
            content_copy = BeautifulSoup(str(main_content), 'html.parser')
            panels_copy = content_copy.find_all('div', class_='panel-default')
            
            # Extrai conteúdo antes dos panels
            first_panel = panels_copy[0]
            before_content = []
            current = first_panel.previous_sibling
            
            while current:
                if isinstance(current, str) and current.isspace():
                    current = current.previous_sibling
                    continue
                    
                if getattr(current, 'name', None) == 'div' and 'margin-top' in current.get('class', []):
                    current = current.previous_sibling
                    continue
                    
                if getattr(current, 'name', None) == 'p' and (not current.get_text(strip=True) or current.get_text(strip=True) == '\xa0'):
                    current = current.previous_sibling
                    continue
                    
                before_content.insert(0, str(current))
                current = current.previous_sibling
            
            if before_content:
                sections.append({
                    'type': 'regular',
                    'content': '\n'.join(before_content)
                })
            
            # Agrupa todos os panels em uma única seção
            panels_html = '\n'.join(str(panel) for panel in all_panels)
            sections.append({
                'type': 'collapsible',
                'content': panels_html
            })
            
            # Extrai conteúdo depois dos panels
            last_panel = panels_copy[-1]
            after_content = []
            current = last_panel.next_sibling
            
            while current:
                if isinstance(current, str) and current.isspace():
                    current = current.next_sibling
                    continue
                    
                if getattr(current, 'name', None) == 'div' and 'margin-top' in current.get('class', []):
                    current = current.next_sibling
                    continue
                    
                if getattr(current, 'name', None) == 'p' and (not current.get_text(strip=True) or current.get_text(strip=True) == '\xa0'):
                    current = current.next_sibling
                    continue
                    
                after_content.append(str(current))
                current = current.next_sibling
            
            if after_content:
                sections.append({
                    'type': 'regular',
                    'content': '\n'.join(after_content)
                })
            
            # Log das seções
            self.logger.info(f"Split content into {len(sections)} sections:")
            for i, section in enumerate(sections):
                self.logger.info(f"Section {i+1}: Type={section['type']}, Length={len(section['content'])}")
            
            return sections
            
        except Exception as e:
            self.logger.error(f"Error splitting content: {str(e)}")
            return [{
                'type': 'regular',
                'content': html_content
            }]


    async def process_mixed_content(self, web_content_creator, title: str, html_content: str, 
                                  folder_id: int, folder_id_dl: int, source_url: str = "") -> List[int]:
        """Processa conteúdo misto após o processamento inicial"""
        try:
            # Divide o conteúdo em seções
            sections = self.split_content(html_content)
            content_ids = []
            
            for index, section in enumerate(sections):
                try:
                    section_title = title if len(sections) == 1 else f"{title} - Parte {index + 1}"
                    self.logger.info(f"Processing section {index + 1}: {section_title} ({section['type']})")
                    
                    if section['type'] == 'regular':
                        # Cria estrutura de conteúdo regular
                        self.logger.info(f"Creating regular structured content in folder: {folder_id}")
                        content_id = await web_content_creator.create_structured_content(
                            section_title,
                            section['content'],
                            folder_id
                        )
                    else:  # section['type'] == 'collapsible'
                        # Cria estrutura de conteúdo colapsável
                        self.logger.info(f"Creating collapsible content in folder: {folder_id}")
                        content_id = await self.collapse_processor.create_collapse_content(
                            web_content_creator,
                            title=section_title,
                            html_content=section['content'],
                            folder_id=folder_id
                        )
                    
                    if content_id:
                        self.logger.info(f"Created content ID {content_id} for section {index + 1}")
                        content_ids.append(content_id)
                    else:
                        self.logger.error(f"Failed to create content for section {index + 1}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing section {index + 1}: {str(e)}")
                    self.logger.error(f"Full error: {traceback.format_exc()}")
                    continue
            
            return content_ids
                
        except Exception as e:
            self.logger.error(f"Error processing mixed content: {str(e)}")
            self.logger.error(f"Full error: {traceback.format_exc()}")
            return []                                                                                                                       