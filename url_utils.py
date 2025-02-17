import logging
import re
from urllib.parse import urlparse

import unidecode

logger = logging.getLogger(__name__)

class UrlUtils:
    def sanitize_content_path(self, title: str) -> str:
        """
        Sanitize a title to create a URL-friendly path
        
        Args:
            title (str): Original title to be sanitized
        
        Returns:
            str: Sanitized URL-friendly path
        """
        try:
            # Normalize the string by removing accents
            normalized = unidecode.unidecode(title)
            
            # Convert to lowercase
            normalized = normalized.lower()
            
            # Replace non-alphanumeric characters with hyphens
            normalized = re.sub(r'[^a-z0-9]+', '-', normalized)
            
            # Remove leading/trailing hyphens
            normalized = normalized.strip('-')
            
            return normalized
        
        except Exception as e:
            # Fallback to a basic sanitization if unidecode fails
            print(f"Error sanitizing path {title}: {str(e)}")
            return re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')
        
    @staticmethod
    def sanitize_url_path(path: str) -> str:
        """
        Sanitiza o caminho da URL removendo acentos e caracteres especiais
        
        Args:
            path (str): Caminho da URL a ser sanitizado
            
        Returns:
            str: Caminho sanitizado
        """
        if not path:
            return ""
            
        try:
            path = unidecode(path.lower())
            path = re.sub(r'[^a-z0-9\-/.]', '', path)
            path = re.sub(r'-+', '-', path)
            path = path.strip('-')
            
            return path
            
        except Exception as e:
            logger.error(f"Error sanitizing path {path}: {str(e)}")
            return path

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