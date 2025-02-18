import asyncio
import logging
from typing import Dict, Optional, List
import aiohttp
from bs4 import BeautifulSoup
from document_creator import DocumentCreator
from web_content_creator import WebContentCreator
from folder_creator import FolderCreator

logger = logging.getLogger(__name__)

class ContentUpdater:
    def __init__(self, config):
        self.config = config
        self.session = None
        self.doc_creator = DocumentCreator(config)
        self.folder_creator = FolderCreator(config)  # Adicionando o FolderCreator
        self.root_folder_id = "5346800"
        
    async def initialize_session(self):
        if self.session:
            await self.session.close()
            
        auth = aiohttp.BasicAuth(
            login=self.config.liferay_user,
            password=self.config.liferay_pass
        )
        
        self.session = aiohttp.ClientSession(
            auth=auth,
            headers={'Content-Type': 'application/json'},
            connector=aiohttp.TCPConnector(ssl=False)
        )
        
        await self.doc_creator.initialize_session()
        await self.folder_creator.initialize_session()

    async def get_content_by_id(self, article_id: str) -> Optional[Dict]:
        try:
            url = f"{self.config.liferay_url}/api/jsonws/journal.journalarticle/get-article"
            
            params = {
                'groupId': self.config.site_id,
                'articleId': article_id,
            }
            
            logger.info(f"Buscando artigo por articleId: {article_id}")
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    content = await response.json()
                    print(content)
                    if content and len(content) > 0:
                        logger.info(f"Artigo {article_id} encontrado com sucesso")
                        return content
                    else:
                        logger.info(f"Nenhum artigo encontrado com articleId: {article_id}")
                        return None
                        
                logger.error(f"Erro ao buscar artigo {article_id}: {response.status}")
                try:
                    error_data = await response.json()
                    logger.error(f"Detalhes do erro: {error_data}")
                except:
                    pass
                return None
                        
        except Exception as e:
            logger.error(f"Erro ao buscar conteúdo {article_id}: {str(e)}")
            return None

    async def get_structured_content_by_key(self, site_id: str, article_id: str, 
                                        fields: str = None, 
                                        nested_fields: str = None,
                                        restrict_fields: str = None) -> Optional[Dict]:
        """
        Recupera um conteúdo estruturado do Liferay usando sua chave.
        
        Argumentos:
            site_id (str): ID do site no Liferay
            article_id (str): Chave do artigo a ser buscado
            fields (str, opcional): Campos específicos a serem incluídos na resposta
            nested_fields (str, opcional): Campos aninhados a serem incluídos
            restrict_fields (str, opcional): Campos a serem restringidos da resposta
            
        Retorna:
            Optional[Dict]: O conteúdo estruturado se encontrado, None caso contrário
        """
        try:
            # Constrói a URL do endpoint usando os parâmetros fornecidos
            url = f"{self.config.liferay_url}/v1.0/sites/{site_id}/structured-contents/by-key/{article_id}"
            
            # Prepara os parâmetros de consulta opcionais
            params = {}
            if fields:
                params['fields'] = fields  # Campos específicos a serem retornados
            if nested_fields:
                params['nestedFields'] = nested_fields  # Campos aninhados a serem incluídos
            if restrict_fields:
                params['restrictFields'] = restrict_fields  # Campos a serem excluídos
                
            logger.info(f"Buscando conteúdo estruturado com key: {article_id}")
            
            # Realiza a requisição HTTP assíncrona
            async with self.session.get(url, params=params) as response:
                if response.status == 200:  # Se a requisição for bem sucedida
                    content = await response.json()
                    
                    if content:  # Se encontrou conteúdo
                        logger.info(f"Conteúdo estruturado com article_id {article_id} encontrado com sucesso")
                        return content
                    else:  # Se não encontrou conteúdo
                        logger.info(f"Nenhum conteúdo estruturado encontrado com article_id: {article_id}")
                        return None
                        
                # Em caso de erro na requisição
                logger.error(f"Erro ao buscar conteúdo estruturado {article_id}: {response.status}")
                try:
                    error_data = await response.json()
                    logger.error(f"Detalhes do erro: {error_data}")
                except:
                    pass  # Ignora erros ao tentar ler detalhes do erro
                return None
                
        except Exception as e:
            # Captura qualquer exceção não prevista
            logger.error(f"Erro ao buscar conteúdo estruturado {article_id}: {str(e)}")
            return None

    
    async def create_article_folder(self, article_title: str) -> Optional[str]:
        try:
            # self, title: str, parent_id: int = 0, folder_type: str = 'journal', hierarchy: List[str] = None
            folder_id = await self.folder_creator.create_folder(
                title=article_title,
                parent_id=self.root_folder_id,
                folder_type='documents'
            )
            
            if folder_id:
                logger.info(f"Pasta criada para o artigo: {article_title} (ID: {folder_id})")
                return folder_id
            
            logger.error(f"Falha ao criar pasta para o artigo: {article_title}")
            return None
            
        except Exception as e:
            logger.error(f"Erro ao criar pasta para o artigo {article_title}: {str(e)}")
            return None
    
    async def update_article_content(self, article_id: str, old_url: str) -> bool:
        try:
            article = await self.get_content_by_id(article_id)
            if not article:
                return False

            articleKey = await self.get_structured_content_by_key(self.config.site_id, article_id)
            print(articleKey)

            content = article[0] if isinstance(article, list) else article
            
            # Parse o XML para extrair o conteúdo HTML
            soup = BeautifulSoup(content.get('content', ''), 'xml')
            
            # Encontra o elemento com o título do artigo
            title_element = soup.find('dynamic-element', attrs={'field-reference': 'call'})
            if title_element:
                article_title = title_element.find('dynamic-content').text
                
            logger.info(f"Criando pasta para o artigo: {article_title}")
            
            article_folder_id = await self.create_article_folder(article_title)
            if not article_folder_id:
                logger.error(f"Não foi possível criar pasta para o artigo {article_id}")
                return False
            
            content_element = soup.find('dynamic-element', attrs={'field-reference': 'content'})
            if not content_element:
                logger.error(f"Artigo {article_id} não possui elemento de conteúdo")
                return False
                
            dynamic_content = content_element.find('dynamic-content', attrs={'language-id': 'pt_BR'})
            if not dynamic_content:
                logger.error(f"Artigo {article_id} não possui dynamic-content")
                return False
                
            html_content = dynamic_content.text
            
            updated_html = await self.process_content_images(
                content=html_content,
                folder_id=int(article_folder_id),
                base_url=old_url
            )
            
            if updated_html != html_content:
                dynamic_content.string = f"<![CDATA[{updated_html}]]>"
                print(f"DDMTemplateKey {article.get('DDMTemplateKey')}")
                # URL da API headless
                update_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{article.get('DDMTemplateKey')}"
                PUT
# /v1.0/sites/{siteId}/structured-contents/by-external-reference-code/{externalReferenceCode}
                # Payload para a API headless
                payload = {
                    "contentStructureId": self.config.content_structure_id,
                    "title": article_title,
                    "contentFields": [
                        {
                            "name": "content",
                            "contentFieldValue": {
                                "data": updated_html
                            }
                        }
                    ]
                }
                
                logger.info(f"Atualizando conteúdo do artigo {article_id}")
                
                async with self.session.put(update_url, json=payload) as response:
                    if response.status in (200, 201):
                        logger.info(f"✓ Artigo {article_id} atualizado com sucesso")
                        return True
                        
                    logger.error(f"Erro ao atualizar artigo {article_id}: {response.status}")
                    try:
                        error_data = await response.json()
                        logger.error(f"Detalhes do erro: {error_data}")
                    except:
                        pass
                    return False
                    
            else:
                logger.info(f"Nenhuma alteração necessária no artigo {article_id}")
                return True

        except Exception as e:
            logger.error(f"Erro ao atualizar artigo {article_id}: {str(e)}")
            return False
    
    async def process_content_images(self, content: str, folder_id: int, base_url: str) -> str:
        soup = BeautifulSoup(content, 'html.parser')

        for img in soup.find_all('img'):
            src = img.get('src')
            if src:
                src = src.replace("sedest", "sedes")
                img['src'] = src

                if 'wp-content' in src or 'wp-conteudo' in src:
                    try:
                        new_url = await self.doc_creator.migrate_document(
                            doc_url=src,
                            folder_id=folder_id,
                            page_url=base_url
                        )
                        if new_url:
                            img['src'] = new_url
                            logger.info(f"Imagem atualizada: {src} -> {new_url}")
                    except Exception as e:
                        logger.error(f"Erro ao processar imagem {src}: {str(e)}")

        return str(soup)

    async def close(self):
        if self.session:
            await self.session.close()
        await self.doc_creator.close()
        await self.folder_creator.close()
