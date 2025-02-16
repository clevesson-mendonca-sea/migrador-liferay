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
        """
        Constrói uma URL completa a partir de uma URL relativa ou absoluta.
        
        Args:
            url (str): URL ou caminho a ser processado
            base_domain (str): Domínio base para URLs relativas
            
        Returns:
            str: URL completa normalizada
        """
        if not url:
            return ""
            
        url = url.lower().strip()
        
        prefixes_to_remove = [
            'usar a url:',
            'usar a url',
            'url:',
            'url'
        ]
        
        # Remove os prefixos conhecidos
        for prefix in prefixes_to_remove:
            if url.startswith(prefix):
                url = url[len(prefix):].strip()
                break
        
        # Remove espaços extras e caracteres indesejados
        url = url.strip('/ \t\n\r')
        
        if url.startswith(('http://', 'https://')):
            return url
            
        if not base_domain:
            logger.warning(f"Base domain not provided for relative URL: {url}")
            return url
            
        base_domain = base_domain.rstrip('/')
        
        try:
            final_url = f"{base_domain}/{url}"
            
            parsed = urlparse(final_url)
            if not all([parsed.scheme, parsed.netloc]):
                logger.warning(f"Invalid URL constructed: {final_url}")
                return url
                
            return final_url
            
        except Exception as e:
            logger.error(f"Error building URL for {url} with base {base_domain}: {str(e)}")
            return url