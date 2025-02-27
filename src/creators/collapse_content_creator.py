import logging
import traceback
from bs4 import BeautifulSoup
import json
import os
import asyncio

logger = logging.getLogger(__name__)

class CollapseContentProcessor:
    def __init__(self, config):
        self.config = config
        self.structure_id = os.getenv('LIFERAY_COLAPSE_STRUCTURE_ID', '')
        self._background_tasks = set()  # Para armazenar tarefas em background
        
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

# Modifique o método _background_process_collapse_images no CollapseContentProcessor:

    async def _background_process_collapse_images(self, web_content_creator, content_id, title, content_fields, source_url, folder_id_dl):
        """
        Processa as imagens dentro de campos colapsáveis em segundo plano e atualiza o conteúdo
        """
        try:
            logger.info(f"[BACKGROUND] Iniciando processamento de imagens para conteúdo colapsável: {title}")
            
            # Verifica se o ContentUpdater já foi inicializado
            if not hasattr(web_content_creator, 'content_updater'):
                from updaters.content_update import ContentUpdater
                web_content_creator.content_updater = ContentUpdater(self.config)
                await web_content_creator.content_updater.initialize_session()
            
            # Processa imagens em cada seção colapsável
            updated = False
            processed_fields = []
            
            for field in content_fields:
                try:
                    # Encontra o campo de conteúdo dentro do campo colapsável
                    for nested_field in field.get("nestedContentFields", []):
                        if nested_field["name"] == "collapse_content":
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
                                # Atualiza o campo com o HTML processado
                                nested_field["contentFieldValue"]["data"] = processed_html
                                updated = True
                                logger.info(f"[BACKGROUND] Processadas imagens em uma seção colapsável de {title}")
                    
                    processed_fields.append(field)
                except Exception as section_error:
                    logger.error(f"[BACKGROUND] Erro processando seção colapsável: {str(section_error)}")
                    # Continua com a próxima seção, incluindo o campo original não processado
                    processed_fields.append(field)
            
            # Se houve alterações, atualiza o conteúdo
            if updated:
                try:
                    # Prepara dados para atualização
                    update_data = {
                        "contentStructureId": self.structure_id,
                        "contentFields": processed_fields,
                        "title": title
                    
                    }
                    
                    # Log detalhado para depuração
                    logger.debug(f"[BACKGROUND] Enviando atualização para conteúdo colapsável {title} (ID: {content_id})")
                    
                    # URL para atualização
                    url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{content_id}"
                    
                    # Executa a atualização
                    status, result = await web_content_creator._controlled_request('put', url, json=update_data)
                    
                    if status in (200, 201, 204):
                        logger.info(f"[BACKGROUND] ✅ Conteúdo colapsável {title} atualizado com imagens processadas")
                    else:
                        error_msg = f"[BACKGROUND] ❌ Erro {status} ao atualizar conteúdo colapsável {title}"
                        logger.error(error_msg)
                        
                        # Log detalhado do erro para diagnóstico
                        logger.error(f"[BACKGROUND] Detalhes do erro: {result}")
                        
                        # Tente enviar apenas campos que não foram processados
                        if status == 400:
                            logger.info(f"[BACKGROUND] Tentando atualização alternativa para {title}")
                            # Usa os campos originais em vez dos processados
                            original_update = {
                                "contentStructureId": self.structure_id,
                                "contentFields": content_fields
                            }
                            
                            alt_status, alt_result = await web_content_creator._controlled_request(
                                'patch', url, json=original_update
                            )
                            
                            if alt_status in (200, 201, 204):
                                logger.info(f"[BACKGROUND] ✅ Atualização alternativa bem-sucedida para {title}")
                            else:
                                logger.error(f"[BACKGROUND] Falha na atualização alternativa: {alt_status}")
                except Exception as update_error:
                    logger.error(f"[BACKGROUND] Exceção na atualização do conteúdo colapsável {title}: {str(update_error)}")
                    logger.error(f"[BACKGROUND] Stack trace: {traceback.format_exc()}")
            else:
                logger.info(f"[BACKGROUND] Nenhuma alteração de imagem necessária para o conteúdo colapsável {title}")
                    
        except Exception as e:
            error_msg = f"[BACKGROUND] Erro ao processar imagens para conteúdo colapsável {title}: {str(e)}"
            logger.error(error_msg)
            logger.error(f"[BACKGROUND] Stack trace: {traceback.format_exc()}")
        finally:
            # Remove a tarefa da lista de tarefas em background
            if task := asyncio.current_task():
                self._background_tasks.discard(task)
            
    def _schedule_background_image_processing(self, web_content_creator, content_id, title, content_fields, source_url, folder_id_dl):
        """
        Agenda o processamento de imagens em background para conteúdo colapsável
        """
        # Cria uma tarefa em background
        task = asyncio.create_task(
            self._background_process_collapse_images(
                web_content_creator, content_id, title, content_fields, source_url, folder_id_dl
            )
        )
        
        # Adiciona à lista de tarefas em background
        self._background_tasks.add(task)
        
        # Log que a tarefa foi agendada
        logger.info(f"Agendado processamento de imagens em background para conteúdo colapsável: {title}")

    async def create_collapse_content(self, web_content_creator, title: str, html_content: str, folder_id: int, source_url: str = None, folder_id_dl: int = None):
        """
        Cria um conteúdo colapsável no Liferay mantendo a estrutura original, 
        e agenda o processamento de imagens em background se source_url e folder_id_dl forem fornecidos
        """
        if not web_content_creator.session:
            await web_content_creator.initialize_session()

        try:
            content_fields = self.process_collapse_content(html_content)
            if not content_fields:
                raise Exception("No valid content fields were generated")

            # Limitar título se necessário (por exemplo, 255 caracteres é um limite comum)
            if len(title) > 255:
                truncated_title = title[:250] + "..."
                logger.info(f"Título truncado para criação de conteúdo: '{title}' -> '{truncated_title}'")
                title = truncated_title

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
                status, result = await web_content_creator._controlled_request('post', url, json=content_data)
                
                if status in (200, 201):
                    content_id = result.get('id')
                    content_key = result.get('key')
                    
                    if content_id:
                        logger.info(f"Successfully created collapse content: {title} (ID: {content_id})")
                        
                        # Se temos informações para processamento de imagens, agendar em background
                        if source_url and folder_id_dl:
                            self._schedule_background_image_processing(
                                web_content_creator, content_id, title, content_fields, source_url, folder_id_dl
                            )
                        
                        return {"id": int(content_id), "key": content_key}
                
                raise Exception(f"Content creation failed with status {status}: {result}")

            return await web_content_creator._retry_operation(create_attempt)
            
        except Exception as e:
            error_msg = f"Error creating collapsible content: {str(e)}"
            logger.error(error_msg)
            if source_url:
                web_content_creator._log_error("Collapse Content Creation", source_url, error_msg, title)
            return {"id": 0, "key": ""}
            
    async def update_collapse_with_processed_images(self, web_content_creator, content_id: int, title: str, content_fields: list, source_url: str, folder_id_dl: int) -> bool:
        """
        Atualiza um conteúdo colapsável existente com imagens processadas
        
        Args:
            web_content_creator: Instância de WebContentCreator
            content_id: ID do conteúdo a ser atualizado
            title: Título do conteúdo
            content_fields: Campos de conteúdo colapsável
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
            error_msg = f"Erro ao agendar atualização de imagens para conteúdo colapsável: {str(e)}"
            logger.error(error_msg)
            web_content_creator._log_error("Collapse Content Update", source_url, error_msg, title)
            return False
            
    async def close(self):
        """Fecha recursos, aguardando tarefas em background"""
        try:
            # Aguardar todas as tarefas em background antes de fechar
            if self._background_tasks:
                logger.info(f"Aguardando {len(self._background_tasks)} tarefas de processamento de colapsáveis...")
                
                # Cria cópia da lista para evitar problemas
                pending_tasks = list(self._background_tasks)
                
                if pending_tasks:
                    # Esperar todas as tarefas com timeout
                    done, pending = await asyncio.wait(
                        pending_tasks, 
                        timeout=60  # Timeout de 60 segundos
                    )
                    
                    if pending:
                        logger.warning(f"{len(pending)} tarefas de colapsáveis não concluídas no timeout")
                    
                    # Cancela as tarefas pendentes
                    for task in pending:
                        task.cancel()
                        
                    logger.info(f"{len(done)} tarefas de colapsáveis concluídas com sucesso")
                    
            logger.info("CollapseContentProcessor fechado com sucesso")
        except Exception as e:
            logger.error(f"Erro ao fechar CollapseContentProcessor: {str(e)}")