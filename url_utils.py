import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class UrlUtils:
    @staticmethod
    def extract_domain(url: str) -> str:
        """Extrai o domínio base de uma URL"""
        if not url:
            return ""
            
        # url = url.replace('Usar a url:', '').strip()
        
        # Se a URL não começa com http, adiciona https://
        if not url.startswith('http'):
            url = 'https://' + url.lstrip('/')
            
        try:
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}"
        except Exception as e:
            logger.error(f"Error extracting domain from {url}: {str(e)}")
            return ""

    @staticmethod
    def build_url(url: str, base_domain: str = "") -> str:
        """Constrói uma URL completa a partir de uma URL relativa ou absoluta"""
        if not url:
            return ""
            
        # Remove qualquer texto 'Usar a url:' que possa ter vindo da planilha
        url = url.replace('Usar a url:', '').strip()
        
        # Se já é uma URL completa, retorna como está
        if url.startswith('http'):
            return url
            
        # Se temos um domínio base e a URL é relativa
        if base_domain:
            return f"{base_domain.rstrip('/')}/{url.lstrip('/')}"
            
        return url