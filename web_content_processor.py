import logging
from bs4 import BeautifulSoup
import re
from functools import lru_cache
from typing import Optional, Dict
from urllib.parse import urlparse
from bs4 import Tag
import asyncio

logger = logging.getLogger(__name__)

class ContentProcessor:
    """Classe responsável por todo o processamento de conteúdo"""
    
    CONTENT_SELECTORS = [
        {'type': 'id', 'value': 'conteudo'},
        {'type': 'class', 'value': 'corpo-principal'},
        {'type': 'class', 'value': 'col-md-8 col-md-offset-1'},
        {'type': 'class', 'value': 'col-md-8 col-md-offset-1 pull-right'},
        {'type': 'class', 'value': 'col-md-8 col-md-offset-1 pull-right col-xs-12'},
        {'type': 'class', 'value': 'col-md-8'},
    ]
    
    BOOTSTRAP_PATTERNS = re.compile('|'.join([
        r'^col-\w+-\d+',
        r'^col-\w+-offset-\d+',
        r'^row',
        r'^container',
        r'^container-fluid',
        r'^offset-',
        r'^pull-',
        r'^push-',
        r'^col-xs-'
    ]))

    IMG_ATTRIBUTES_TO_REMOVE = [
        'srcset',
        'sizes',
        'loading',
        'fetchpriority',
        'decoding'
    ]

    def __init__(self, web_content_creator):
        self.creator = web_content_creator
        self.cache = web_content_creator.cache
        self.url_utils = web_content_creator.url_utils
        self.document_creator = web_content_creator.document_creator
        self.semaphore = asyncio.Semaphore(5)

    @lru_cache(maxsize=1000)
    def _clean_url(self, url: str, base_domain: str) -> str:
        """
        Limpa e normaliza URLs com cache.
        Apenas URLs do mesmo domínio são processadas, outras permanecem inalteradas.
        """
        if not url:
            return url
            
        if url.startswith('/'):
            return url
            
        try:
            parsed = urlparse(url)
            
            # Se não tem netloc (domínio), provavelmente é relativo
            if not parsed.netloc:
                return url
                
            url_domain = parsed.netloc.lower()
            base_domain_parsed = urlparse(base_domain)
            base_netloc = base_domain_parsed.netloc.lower() if base_domain_parsed.netloc else base_domain.lower()
            
            # Se não for do mesmo domínio, retorna a URL original sem modificações
            if url_domain != base_netloc:
                return url
                
            # Se for do mesmo domínio, converte para relativo
            path = parsed.path
            if parsed.query:
                path = f"{path}?{parsed.query}"
            if parsed.fragment:
                path = f"{path}#{parsed.fragment}"
            return f"/{path.lstrip('/')}"
            
        except Exception as e:
            logger.error(f"Error cleaning URL {url}: {str(e)}")
            return url

    def _clean_img_attributes(self, soup: BeautifulSoup) -> None:
        """Remove atributos desnecessários das tags de imagem"""
        for img in soup.find_all('img'):
            for attr in self.IMG_ATTRIBUTES_TO_REMOVE:
                if attr in img.attrs:
                    del img[attr]

    def _clean_content(self, html_content: str) -> str:
        """Limpa o conteúdo HTML de elementos desnecessários"""
        if not html_content:
            return ""
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove date/time patterns
        if date_div := soup.find('div', style='font-size:14px;'):
            if re.search(r'\d{2}/\d{2}/\d{2}\s+às\s+\d{2}h\d{2}', date_div.text):
                date_div.decompose()
        
        # Limpa atributos de imagem
        self._clean_img_attributes(soup)
        
        # Fix email links and clean up in one pass
        for element in soup.find_all(['a', 'p', 'div']):
            if element.name == 'a':
                href = element.get('href', '')
                if href.startswith('/') and '@' in href:
                    element['href'] = f'mailto:{href.lstrip("/")}'
            elif element.name == 'p':
                if element.string and (element.string.isspace() or element.string == '\xa0'):
                    element.decompose()
            elif element.name == 'div' and 'margin-top-20' in element.get('class', []):
                element.decompose()
        
        return str(soup)

    async def _process_url(self, tag: Tag, attr: str, url: str, base_domain: str, folder_id: int, base_url: str) -> None:
        """Processa URLs individuais com cache"""
        async with self.semaphore:
            try:
                if cached_url := self.cache.get_url(url):
                    tag[attr] = cached_url
                    return

                if any(wp_path in url for wp_path in ['/wp-conteudo', '/wp-content']):
                    full_url = url
                    if not url.startswith(('http://', 'https://')):
                        full_url = f"{base_domain.rstrip('/')}/{url.lstrip('/')}"
                    
                    migrated_url = await self.creator._retry_operation(
                        self.document_creator.migrate_document,
                        doc_url=full_url,
                        folder_id=folder_id,
                        page_url=base_url
                    )

                    if migrated_url:
                        relative_url = self._clean_url(migrated_url, base_domain)
                        self.cache.add_url(url, relative_url)
                        tag[attr] = relative_url
                    else:
                        self.cache.mark_failed(url)
                else:
                    # Processa outras URLs mantendo externas como absolutas
                    cleaned_url = self._clean_url(url, base_domain)
                    tag[attr] = cleaned_url

            except Exception as e:
                logger.error(f"Error processing URL {url}: {str(e)}")
                self.cache.mark_failed(url)

    async def process_content(self, html_content: str, base_url: str, folder_id: Optional[int] = None) -> str:
        """Processa conteúdo HTML completo"""
        if not html_content:
            return ""

        soup = BeautifulSoup(html_content, 'html.parser')
        base_domain = self.url_utils.extract_domain(base_url)

        self._clean_img_attributes(soup)

        # Process all tags in a single pass
        async def process_tags():
            tasks = []
            for tag in soup.find_all(['a', 'img', 'link', 'script']):
                attr = next((a for a in ['href', 'src', 'data-src'] if tag.get(a)), None)
                if attr and (url := tag.get(attr)):
                    tasks.append(self._process_url(tag, attr, url, base_domain, folder_id, base_url))
            
            return await asyncio.gather(*tasks) if tasks else []

        await process_tags()
        return str(soup)

    def _clean_first_div_bootstrap(self, html_content: str) -> str:
        """Remove classes Bootstrap da primeira div"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            first_div = soup.find('div')
            
            if not first_div:
                return html_content
            
            if 'class' in first_div.attrs:
                classes = first_div.get('class', [])
                if isinstance(classes, str):
                    classes = classes.split()
                
                cleaned_classes = [
                    c for c in classes 
                    if not self.BOOTSTRAP_PATTERNS.match(c)
                ]
                
                if cleaned_classes:
                    first_div['class'] = cleaned_classes
                else:
                    del first_div['class']
            
            return str(soup)
            
        except Exception as e:
            logger.error(f"Error cleaning bootstrap classes: {str(e)}")
            return html_content

    def _is_collapsible_content(self, html_content: str) -> bool:
        """Verifica se o conteúdo é colapsável"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Verifica panels com classe específica
            panels = soup.find_all('div', class_=['panel', 'panel-default', 'panel-success'])
            if not panels:
                return False
            
            # Verifica estrutura de cada painel
            for panel in panels:
                has_heading = bool(panel.find('div', class_='panel-heading'))
                has_collapse = bool(panel.find('div', class_='panel-collapse'))
                has_body = bool(panel.find('div', class_='panel-body'))
                has_title = bool(panel.find(['h4', 'h3', 'p'], class_='panel-title'))
                
                if (has_heading and (has_collapse or has_body)) or (has_title and has_body):
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking collapsible content: {str(e)}")
            return False

    async def fetch_and_process_content(self, url: str, folder_id: Optional[int] = None) -> Dict[str, any]:
        """Busca e processa conteúdo completo"""
        try:
            # Busca conteúdo
            html_content = await self.creator.fetch_content(url)
            if not html_content:
                return {"success": False, "error": "No content found"}

            # Processa conteúdo
            processed_content = await self.process_content(html_content, url, folder_id)
            if not processed_content:
                return {"success": False, "error": "Failed to process content"}
            
            # Limpa classes bootstrap
            cleaned_content = self._clean_first_div_bootstrap(processed_content)

            # Identifica tipo de conteúdo
            is_fully_collapsible = self._is_collapsible_content(cleaned_content)
            
            soup = BeautifulSoup(cleaned_content, 'html.parser')
            has_some_collapsible = any(
                self._is_collapsible_content(str(elem)) 
                for elem in soup.find_all('div')
            )

            return {
                "success": True,
                "content": cleaned_content,
                "is_collapsible": is_fully_collapsible,
                "has_mixed_content": has_some_collapsible and not is_fully_collapsible
            }

        except Exception as e:
            logger.error(f"Error processing content from {url}: {str(e)}")
            return {"success": False, "error": str(e)}