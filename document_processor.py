import asyncio
from asyncio.log import logger
from typing import Dict, List, Optional


class DocumentUrlProcessor:
    def __init__(self, session, config):
        self.session = session
        self.config = config
        self.url_cache = {}  # Cache de URLs original -> nova
        self.filename_cache = {}  # Cache de nomes de arquivo -> documento
        self.processed_urls = set()  # URLs já processadas
        self.failed_urls = set()  # URLs que falharam
        
    async def find_existing_document_by_name(self, filename: str, folder_id: Optional[int] = None) -> Optional[str]:
        """Busca documento existente pelo nome"""
        # Verifica primeiro no cache
        if filename in self.filename_cache:
            return self.filename_cache[filename]
            
        try:
            if folder_id:
                search_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/document-folders/{folder_id}/documents"
            else:
                search_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/documents"
            
            params = {
                'filter': f"title eq '{filename}'",
                'fields': 'id,contentUrl',
                'page': 1,
                'pageSize': 1
            }
            
            async with self.session.get(search_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    items = data.get('items', [])
                    if items:
                        content_url = items[0].get('contentUrl')
                        if content_url:
                            # Armazena no cache
                            self.filename_cache[filename] = content_url
                            return content_url
            return None
        except Exception as e:
            logger.error(f"Erro ao buscar documento existente: {str(e)}")
            return None

    async def process_url(self, url: str, folder_id: Optional[int] = None) -> Optional[str]:
        """Processa uma URL e retorna a nova URL ou None"""
        # Verifica se já está no cache
        if url in self.url_cache:
            return self.url_cache[url]
            
        # Verifica se já foi processada e falhou
        if url in self.failed_urls:
            return None
            
        # Verifica se já foi processada
        if url in self.processed_urls:
            return None
            
        try:
            # Extrai o nome do arquivo
            filename = self._extract_filename(url)
            
            # Verifica se já existe um documento com esse nome
            existing_url = await self.find_existing_document_by_name(filename, folder_id)
            if existing_url:
                # Armazena no cache e retorna
                self.url_cache[url] = existing_url
                self.processed_urls.add(url)
                return existing_url
                
            # Se não existe, processa normalmente
            new_url = await self.migrate_document(url, folder_id)
            if new_url:
                # Armazena no cache
                self.url_cache[url] = new_url
                self.filename_cache[filename] = new_url
                self.processed_urls.add(url)
                return new_url
                
            # Se falhou, marca como falha
            self.failed_urls.add(url)
            return None
            
        except Exception as e:
            logger.error(f"Erro ao processar URL {url}: {str(e)}")
            self.failed_urls.add(url)
            return None

    async def process_batch(self, urls: List[str], folder_id: Optional[int] = None) -> Dict[str, str]:
        """Processa um lote de URLs e retorna um dicionário com os mapeamentos"""
        results = {}
        
        # Primeiro verifica todas as URLs no cache
        for url in urls:
            if url in self.url_cache:
                results[url] = self.url_cache[url]
                continue
                
            if url in self.failed_urls:
                continue
                
            # Extrai o nome do arquivo
            filename = self._extract_filename(url)
            
            # Verifica se já existe um documento com esse nome
            existing_url = await self.find_existing_document_by_name(filename, folder_id)
            if existing_url:
                self.url_cache[url] = existing_url
                results[url] = existing_url
                continue
            
        # Processa as URLs restantes em paralelo
        remaining_urls = [url for url in urls if url not in results and url not in self.failed_urls]
        if remaining_urls:
            sem = asyncio.Semaphore(5)  # Limita a 5 requisições simultâneas
            
            async def process_with_sem(url):
                async with sem:
                    return url, await self.process_url(url, folder_id)
            
            tasks = [process_with_sem(url) for url in remaining_urls]
            processed = await asyncio.gather(*tasks)
            
            for url, new_url in processed:
                if new_url:
                    results[url] = new_url
        
        return results

    def get_cached_url(self, url: str) -> Optional[str]:
        """Retorna a URL do cache se existir"""
        return self.url_cache.get(url)

    def is_failed(self, url: str) -> bool:
        """Verifica se a URL falhou anteriormente"""
        return url in self.failed_urls

    def clear_cache(self):
        """Limpa todos os caches"""
        self.url_cache.clear()
        self.filename_cache.clear()
        self.processed_urls.clear()
        self.failed_urls.clear()