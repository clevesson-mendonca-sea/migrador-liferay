from typing import Optional

class ContentCache:
    def __init__(self):
        # Cache principal
        self.content_cache = {}  # title -> content_id
        self.page_cache = {}     # title -> page_id
        self.url_cache = {}      # original_url -> migrated_url
        self.folder_cache = {}   # hierarchy_key -> folder_id
        
        # Controle de processamento
        self.processed_urls = set()  # URLs já processadas
        self.failed_urls = set()     # URLs que falharam
        self.retry_count = {}        # URL -> contagem de tentativas
        
        # Cache de documentos
        self.filename_cache = {}     # filename -> document_info
        self.document_cache = {}     # doc_id -> document_info
        
    def add_content(self, title: str, content_id: int):
        self.content_cache[title.lower()] = content_id

    def get_content(self, title: str) -> Optional[int]:
        return self.content_cache.get(title.lower())

    def add_page(self, title: str, page_id: int):
        self.page_cache[title.lower()] = page_id

    def get_page(self, title: str) -> Optional[int]:
        return self.page_cache.get(title.lower())

    def add_url(self, original_url: str, migrated_url: str):
        self.url_cache[original_url] = migrated_url
        self.processed_urls.add(original_url)

    def get_url(self, url: str) -> Optional[str]:
        return self.url_cache.get(url)

    def add_folder(self, hierarchy_key: tuple, folder_id: int):
        self.folder_cache[hierarchy_key] = folder_id

    def get_folder(self, hierarchy_key: tuple) -> Optional[int]:
        return self.folder_cache.get(hierarchy_key)

    def add_document(self, filename: str, doc_info: dict):
        self.filename_cache[filename] = doc_info
        if 'id' in doc_info:
            self.document_cache[doc_info['id']] = doc_info

    def get_document_by_filename(self, filename: str) -> Optional[dict]:
        return self.filename_cache.get(filename)

    def get_document_by_id(self, doc_id: str) -> Optional[dict]:
        return self.document_cache.get(doc_id)

    def mark_processed(self, url: str):
        self.processed_urls.add(url)

    def is_processed(self, url: str) -> bool:
        return url in self.processed_urls

    def mark_failed(self, url: str):
        self.failed_urls.add(url)
        self.processed_urls.add(url)

    def is_failed(self, url: str) -> bool:
        return url in self.failed_urls

    def increment_retry(self, url: str) -> int:
        count = self.retry_count.get(url, 0) + 1
        self.retry_count[url] = count
        return count

    def get_retry_count(self, url: str) -> int:
        return self.retry_count.get(url, 0)

    def clear_retries(self, url: str):
        self.retry_count.pop(url, None)

    def clear_all(self):
        """Limpa todos os caches"""
        self.content_cache.clear()
        self.page_cache.clear()
        self.url_cache.clear()
        self.folder_cache.clear()
        self.processed_urls.clear()
        self.failed_urls.clear()
        self.retry_count.clear()
        self.filename_cache.clear()
        self.document_cache.clear()