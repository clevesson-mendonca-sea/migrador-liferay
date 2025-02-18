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

    async def get_content_by_id(self, article_id: str) -> Optional[Dict]:
        try:
            url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{article_id}"
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                logger.error(f"Erro ao buscar conteúdo {article_id}: {response.status}")
                return None
        except Exception as e:
            logger.error(f"Erro ao buscar conteúdo {article_id}: {str(e)}")
            return None

    async def process_content_images(self, content: str, folder_id: int, base_url: str) -> str:
        soup = BeautifulSoup(content, 'html.parser')
        
        for img in soup.find_all('img'):
            src = img.get('src')
            if src and ('wp-content' in src or 'wp-conteudo' in src):
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

    async def update_article_content(self, article_id: str, old_url: str) -> bool:
        try:
            # Busca o conteúdo atual
            content = await self.get_content_by_id(article_id)
            if not content:
                return False

            # Processa campos de conteúdo
            content_fields = content.get('contentFields', [])
            updated = False

            for field in content_fields:
                if field.get('name') == 'content':
                    field_value = field.get('contentFieldValue', {})
                    html_content = field_value.get('data', '')
                    
                    if html_content:
                        # Processa imagens no conteúdo usando a pasta raiz
                        updated_content = await self.process_content_images(
                            content=html_content,
                            folder_id=int(self.root_folder_id),
                            base_url=old_url
                        )
                        
                        if updated_content != html_content:
                            field_value['data'] = updated_content
                            updated = True

            if updated:
                # Atualiza o conteúdo
                update_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{article_id}"
                async with self.session.put(update_url, json=content) as response:
                    if response.status in (200, 201):
                        logger.info(f"Conteúdo {article_id} atualizado com sucesso")
                        return True
                    logger.error(f"Erro ao atualizar conteúdo {article_id}: {response.status}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Erro ao atualizar artigo {article_id}: {str(e)}")
            return False

    async def update_from_spreadsheet_data(self, pages: List[Dict]) -> Dict[str, bool]:
        results = {}
        
        for page in pages:
            article_id = page.get('destination', '').strip()
            if article_id and article_id.isdigit():
                logger.info(f"\nAtualizando artigo: {article_id}")
                logger.info(f"URL original: {page.get('url', '')}")
                logger.info(f"Título: {page.get('title', '')}")
                
                success = await self.update_article_content(
                    article_id=article_id,
                    old_url=page.get('url', '')
                )
                results[article_id] = success

        return results

    async def close(self):
        if self.session:
            await self.session.close()
        await self.doc_creator.close()