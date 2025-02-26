import traceback
from typing import Dict, List
from bs4 import BeautifulSoup
import logging
from creators.collapse_content_creator import CollapseContentProcessor

class MixedContentProcessor:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.collapse_processor = CollapseContentProcessor(config)
        if not hasattr(self, 'page_creator'):
            from creators.page_creator import PageCreator
            self.page_creator = PageCreator(config)
        
        from creators.tab_content_creator import TabContentProcessor
        self.tab_processor = TabContentProcessor(config)
            
    def split_content(self, html_content: str) -> List[Dict[str, str]]:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            tab_list = soup.find('ul', class_='nav-tabs')
            tab_content = soup.find('div', class_='tab-content')
            
            # Detecta painéis colapsáveis
            panels = soup.find_all('div', class_=lambda c: c and any(panel_cls in c for panel_cls in ['panel', 'panel-default', 'panel-success']))
            
            # Se houver abas, trata de forma diferente
            if tab_list and tab_content:
                # Verifica se há mais de uma aba
                tab_items = tab_list.find_all('li')
                if len(tab_items) > 1:
                    return [{
                        'type': 'tabs',
                        'content': html_content
                    }]
            
            if not panels:
                # Se não há painéis, retorna todo o conteúdo como regular
                return [{'type': 'regular', 'content': html_content}]
            
            # Cria uma cópia da sopa para trabalhar
            content_soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove todos os painéis da sopa de conteúdo
            for panel in panels:
                # Encontra o elemento correspondente na sopa de conteúdo
                panel_id = panel.get('id', '')
                if panel_id:
                    panel_to_remove = content_soup.find('div', id=panel_id)
                else:
                    # Tenta encontrar pelo conteúdo ou estrutura
                    panel_to_remove = None
                    for potential_panel in content_soup.find_all('div', class_=lambda c: c and any(panel_cls in c for panel_cls in ['panel', 'panel-default', 'panel-success'])):
                        if str(potential_panel) == str(panel):
                            panel_to_remove = potential_panel
                            break
                
                if panel_to_remove:
                    panel_to_remove.decompose()
            
            # O que sobrou é o conteúdo regular
            regular_content = str(content_soup)
            
            # Cria as seções
            sections = []
            
            # Adiciona o conteúdo regular se não estiver vazio
            if regular_content.strip():
                sections.append({
                    'type': 'regular',
                    'content': regular_content
                })
            
            # Adiciona os painéis como seção colapsável
            if panels:
                panels_html = ''.join(str(panel) for panel in panels)
                sections.append({
                    'type': 'collapsible',
                    'content': panels_html
                })
            
            return sections
        
        except Exception as e:
            self.logger.error(f"Error splitting content: {str(e)}")
            return [{'type': 'regular', 'content': html_content}]

    async def process_mixed_content(self, web_content_creator, title: str, html_content: str, 
                            folder_id: int, folder_id_dl: int, source_url: str = "") -> List[int]:
        """Processa conteúdo misto após o processamento inicial"""
        try:
            # Divide o conteúdo em seções
            sections = self.split_content(html_content)
            
            content_ids = []
            
            # Tratamento para conteúdo de abas (adicionado)
            if len(sections) == 1 and sections[0]['type'] == 'tabs':
                content_id = await self.tab_processor.create_tab_content(
                    web_content_creator, title, sections[0]['content'], folder_id
                )
                
                if content_id:
                    content_ids.append(content_id)
                
                return content_ids
            
            # Verifica se temos múltiplas seções e atualiza o layout da página se necessário
            if len(sections) > 1:
                # Buscar dados da página
                page_data = await web_content_creator.find_page_by_title_or_id(title)
                
                if page_data:
                    # Temos múltiplas seções, precisamos garantir portlets suficientes
                    num_sections = len(sections)
                    page_id = page_data.get('id')
                    plid = page_id  # Ou use page_data.get('plid', page_id) se plid estiver disponível
                    
                    # Atualize o layout da página para incluir múltiplos portlets
                    try:
                        # Verifica se o page_creator está disponível
                        if hasattr(web_content_creator, 'page_creator'):
                            column_type = "1_column"  # Ou escolha o tipo de coluna adequado
                            
                            # Atualize o layout para incluir múltiplos portlets
                            success = await web_content_creator.page_creator._update_page_layout(
                                page_id, plid, column_type,
                                hierarchy=[], 
                                menu_title=title,
                                url_vinculada="",
                                num_portlets=num_sections  # Passar o número de portlets necessários
                            )
                            
                            if success:
                                self.logger.info(f"Layout da página atualizado com {num_sections} portlets")
                                # Buscar a página novamente para obter os portlets atualizados
                                page_data = await web_content_creator.find_page_by_title_or_id(page_id)
                            else:
                                self.logger.warning("Falha ao atualizar layout da página")
                        else:
                            self.logger.warning("page_creator não disponível para atualizar layout")
                    except Exception as layout_error:
                        self.logger.error(f"Erro ao atualizar layout: {str(layout_error)}")
            
            # Se houver apenas uma seção, usa o título original
            if len(sections) == 1:
                section = sections[0]
                if section['type'] == 'regular':
                    content_id = await web_content_creator.create_structured_content(
                        title, section['content'], folder_id
                    )
                else:
                    # Usa informação de tipo colapsável se disponível
                    collapsible_type = section.get('collapsible_type', 'panel')
                    self.logger.info(f"Criando conteúdo colapsável único com tipo: {collapsible_type}")
                    content_id = await self.collapse_processor.create_collapse_content(
                        web_content_creator, title, section['content'], folder_id
                    )
                
                if content_id:
                    content_ids.append(content_id)
                return content_ids
            
            # Se houver múltiplas seções, trata cada uma adequadamente
            regular_section_count = 0
            collapsible_section_count = 0
            
            for index, section in enumerate(sections):
                try:
                    # Determina um título apropriado para a seção
                    if section['type'] == 'regular':
                        regular_section_count += 1
                        section_title = f"{title} - Parte {regular_section_count}" if regular_section_count > 1 else title
                    else:  # collapsible
                        collapsible_section_count += 1
                        # Tenta obter um título mais descritivo para a seção colapsável
                        section_soup = BeautifulSoup(section['content'], 'html.parser')
                        panel_title = section_soup.select_one('.panel-title')
                        
                        if panel_title and panel_title.get_text(strip=True):
                            section_title = f"{title} - {panel_title.get_text(strip=True)}"
                        else:
                            section_title = f"{title} - Seção Colapsável {collapsible_section_count}"
                    
                    self.logger.info(f"Processing section {index + 1}: {section_title} ({section['type']})")
                    
                    if section['type'] == 'regular':
                        content_id = await web_content_creator.create_structured_content(
                            section_title, section['content'], folder_id
                        )
                    else:
                        # Verificação adicional para garantir que é realmente colapsável
                        collapsible_type = section.get('collapsible_type', None)
                        if collapsible_type == 'none':
                            # Se foi marcado como não-colapsável, mas está na seção colapsável,
                            # vamos verificar novamente
                            section_soup = BeautifulSoup(section['content'], 'html.parser')
                            panels = section_soup.select('div.panel, div.panel-default, div.panel-success')
                            
                            if not panels:
                                self.logger.warning(f"Seção {index+1} marcada como colapsável, mas nenhum painel encontrado. Tratando como regular.")
                                content_id = await web_content_creator.create_structured_content(
                                    section_title, section['content'], folder_id
                                )
                            else:
                                # Painéis encontrados, tratar como colapsável
                                self.logger.info(f"Forçando tipo 'panel' para seção {index+1}")
                                content_id = await self.collapse_processor.create_collapse_content(
                                    web_content_creator, section_title, section['content'], folder_id
                                )
                        else:
                            # Tudo certo, processar como colapsável
                            content_id = await self.collapse_processor.create_collapse_content(
                                web_content_creator, section_title, section['content'], folder_id
                            )
                    
                    if content_id:
                        content_ids.append(content_id)
                    
                except Exception as e:
                    self.logger.error(f"Error processing section {index + 1}: {str(e)}")
                    self.logger.error(traceback.format_exc())
                    continue
            
            return content_ids
                
        except Exception as e:
            self.logger.error(f"Error processing mixed content: {str(e)}")
            self.logger.error(traceback.format_exc())
            return []