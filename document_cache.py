class DocumentCache:
    def __init__(self):
        self.url_mapping = {}  # Mapeia URLs originais para URLs do Liferay
        self.filename_mapping = {}  # Mapeia nomes de arquivo para IDs/URLs
        self.processed_urls = set()  # URLs já processadas
        self.failed_urls = set()  # URLs que falharam
        self.folder_cache = {}  # Cache de IDs de pastas
        
    def add_url_mapping(self, original_url: str, liferay_url: str):
        self.url_mapping[original_url] = liferay_url
        self.processed_urls.add(original_url)
        
    def add_filename_mapping(self, filename: str, doc_info: dict):
        self.filename_mapping[filename] = doc_info
        
    def get_by_url(self, url: str) -> str | None:
        return self.url_mapping.get(url)
        
    def get_by_filename(self, filename: str) -> dict | None:
        return self.filename_mapping.get(filename)
        
    def is_processed(self, url: str) -> bool:
        return url in self.processed_urls
        
    def mark_failed(self, url: str):
        self.failed_urls.add(url)
        self.processed_urls.add(url)
        
    def is_failed(self, url: str) -> bool:
        return url in self.failed_urls