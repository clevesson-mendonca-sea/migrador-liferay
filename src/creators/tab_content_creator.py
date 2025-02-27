import logging
from bs4 import BeautifulSoup
import json
import os
import asyncio

class TabContentProcessor:
    def __init__(self, config):
        """
        Inicializa o processador de conteúdo de abas
        
        :param config: Configuração com detalhes de conexão e estrutura
        """
        self.config = config
        self.tab_structure_id = os.getenv('LIFERAY_TAB_STRUCTURE_ID', '')
        self.logger = logging.getLogger(__name__)
        self._background_tasks = set()  # Para armazenar tarefas em background

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

    async def _background_process_tab_images(self, web_content_creator, content_id, title, content_fields, source_url, folder_id_dl):
        """
        Processa as imagens dentro de campos de abas em segundo plano e atualiza o conteúdo
        """
        try:
            self.logger.info(f"[BACKGROUND] Iniciando processamento de imagens para conteúdo de abas: {title}")
            
            # Verifica se o ContentUpdater já foi inicializado
            if not hasattr(web_content_creator, 'content_updater'):
                from updaters.content_update import ContentUpdater
                web_content_creator.content_updater = ContentUpdater(self.config)
                await web_content_creator.content_updater.initialize_session()
            
            # Processa imagens em cada aba
            updated = False
            processed_fields = []
            
            for field in content_fields:
                processed_nested_fields = []
                
                for nested_field in field.get("nestedContentFields", []):
                    if nested_field["name"] == "content":
                        # Extrai o conteúdo HTML
                        content_html = nested_field["contentFieldValue"]["data"]
                        
                        # Processa imagens usando o ContentUpdater
                        processed_html = await web_content_creator.content_updater.process_content_images(
                            content=content_html,
                            folder_id=folder_id_dl,
                            base_url=source_url
                        )
                        
                        # Verifica se houve mudanças
                        if processed_html != content_html:
                            # Cria uma cópia do campo com o HTML processado
                            updated_field = {
                                "name": nested_field["name"],
                                "contentFieldValue": {
                                    "data": processed_html
                                }
                            }
                            processed_nested_fields.append(updated_field)
                            updated = True
                        else:
                            # Mantém o campo original
                            processed_nested_fields.append(nested_field)
                    else:
                        # Mantém outros campos inalterados
                        processed_nested_fields.append(nested_field)
                
                # Atualiza o campo com os nested fields processados
                processed_field = {
                    "name": field["name"],
                    "nestedContentFields": processed_nested_fields
                }
                processed_fields.append(processed_field)
            
            # Se houve alterações, atualiza o conteúdo
            if updated:
                # Prepara dados para atualização
                update_data = {
                    "contentStructureId": self.tab_structure_id,
                    "contentFields": processed_fields,
                    "title": title
                }
                
                url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{content_id}"
                
                status, result = await web_content_creator._controlled_request('put', url, json=update_data)
                
                if status in (200, 201, 204):
                    self.logger.info(f"[BACKGROUND] ✅ Conteúdo de abas {title} atualizado com imagens processadas")
                else:
                    self.logger.error(f"[BACKGROUND] ❌ Erro ao atualizar conteúdo de abas {title}: {status}")
            else:
                self.logger.info(f"[BACKGROUND] Nenhuma alteração de imagem necessária para o conteúdo de abas {title}")
                
        except Exception as e:
            error_msg = f"[BACKGROUND] Erro ao processar imagens para conteúdo de abas {title}: {str(e)}"
            self.logger.error(error_msg)
            if hasattr(web_content_creator, '_log_error'):
                web_content_creator._log_error("Tab Images Background", source_url, error_msg, title)
        finally:
            # Remove a tarefa da lista de tarefas em background
            if task := asyncio.current_task():
                self._background_tasks.discard(task)

    def _schedule_background_image_processing(self, web_content_creator, content_id, title, content_fields, source_url, folder_id_dl):
        """
        Agenda o processamento de imagens em background para conteúdo de abas
        """
        # Cria uma tarefa em background
        task = asyncio.create_task(
            self._background_process_tab_images(
                web_content_creator, content_id, title, content_fields, source_url, folder_id_dl
            )
        )
        
        # Adiciona à lista de tarefas em background
        self._background_tasks.add(task)
        
        # Log que a tarefa foi agendada
        self.logger.info(f"Agendado processamento de imagens em background para conteúdo de abas: {title}")

    async def create_tab_content(self, web_content_creator, title: str, html_content: str, folder_id: int, source_url: str = None, folder_id_dl: int = None):
        """
        Cria conteúdo de abas no Liferay e agenda processamento de imagens em background 
        se source_url e folder_id_dl forem fornecidos
        
        :param web_content_creator: Instância do criador de conteúdo web
        :param title: Título do conteúdo
        :param html_content: Conteúdo HTML com abas
        :param folder_id: ID da pasta onde o conteúdo será criado
        :param source_url: URL de origem para processamento de imagens
        :param folder_id_dl: ID da pasta de documentos para armazenar imagens processadas
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
                status, result = await web_content_creator._controlled_request('post', url, json=content_data)
                
                if status in (200, 201):
                    content_id = result.get('id')
                    content_key = result.get('key')
                    
                    if content_id:
                        self.logger.info(f"Successfully created tab content: {title} (ID: {content_id})")
                        
                        # Se temos informações para processamento de imagens, agendar em background
                        if source_url and folder_id_dl:
                            self._schedule_background_image_processing(
                                web_content_creator, content_id, title, content_fields, source_url, folder_id_dl
                            )
                        
                        return {"id": int(content_id), "key": content_key}
                
                error_msg = f"Content creation failed with status {status}: {result}"
                self.logger.error(error_msg)
                if hasattr(web_content_creator, '_log_error') and source_url:
                    web_content_creator._log_error("Tab Content Creation", source_url, error_msg, title)
                raise Exception(error_msg)

            return await web_content_creator._retry_operation(create_attempt)
        except Exception as e:
            self.logger.error(f"Error creating tab content: {str(e)}")
            return {"id": 0, "key": ""}
            
    async def update_tab_with_processed_images(self, web_content_creator, content_id: int, title: str, content_fields: list, source_url: str, folder_id_dl: int) -> bool:
        """
        Atualiza um conteúdo de abas existente com imagens processadas
        
        Args:
            web_content_creator: Instância de WebContentCreator
            content_id: ID do conteúdo a ser atualizado
            title: Título do conteúdo
            content_fields: Campos de conteúdo de abas
            source_url: URL de origem para processamento
            folder_id_dl: ID da pasta de documentos
            
        Returns:
            bool: True se a atualização foi bem-sucedida
        """
        try:
            # Agenda o processamento em background e retorna imediatamente
            self._schedule_background_image_processing(
                web_content_creator, content_id, title, content_fields, source_url, folder_id_dl
            )
            return True
            
        except Exception as e:
            error_msg = f"Erro ao agendar atualização de imagens para conteúdo de abas: {str(e)}"
            self.logger.error(error_msg)
            if hasattr(web_content_creator, '_log_error'):
                web_content_creator._log_error("Tab Content Update", source_url, error_msg, title)
            return False
            
    async def close(self):
        """Fecha recursos, aguardando tarefas em background"""
        try:
            # Aguardar todas as tarefas em background antes de fechar
            if self._background_tasks:
                self.logger.info(f"Aguardando {len(self._background_tasks)} tarefas de processamento de abas...")
                
                # Cria cópia da lista para evitar problemas
                pending_tasks = list(self._background_tasks)
                
                if pending_tasks:
                    # Esperar todas as tarefas com timeout
                    done, pending = await asyncio.wait(
                        pending_tasks, 
                        timeout=60  # Timeout de 60 segundos
                    )
                    
                    if pending:
                        self.logger.warning(f"{len(pending)} tarefas de abas não concluídas no timeout")
                    
                    # Cancela as tarefas pendentes
                    for task in pending:
                        task.cancel()
                        
                    self.logger.info(f"{len(done)} tarefas de abas concluídas com sucesso")
                    
            self.logger.info("TabContentProcessor fechado com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao fechar TabContentProcessor: {str(e)}")