import logging
from bs4 import BeautifulSoup, Tag
import re
from functools import lru_cache
from typing import List, Optional, Dict, Set, Tuple, Any, Union
from urllib.parse import urlparse
import asyncio
from collections import defaultdict

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
    
    DATE_PATTERN = re.compile(r'\d{2}/\d{2}/\d{2}\s+às\s+\d{2}h\d{2}')
    EMAIL_HREF_PATTERN = re.compile(r'^/[^/]+@[^/]+')
    EMPTY_PARAGRAPH_PATTERN = re.compile(r'^\s*(&nbsp;|\xa0)?\s*$')

    IMG_ATTRIBUTES_TO_REMOVE = {
        'srcset', 'sizes', 'loading', 'fetchpriority', 'decoding'
    }
    
    CLASS_SELECTORS_SET = {
        'corpo-principal',
        'col-md-8 col-md-offset-1',
        'col-md-8 col-md-offset-1 pull-right',
        'col-md-8 col-md-offset-1 pull-right col-xs-12',
        'col-md-8'
    }
    
    DOCUMENT_PATHS = {'/wp-conteudo', '/wp-content'}
    HEADING_TAGS = {'h1', 'h2', 'h3', 'h4'}

    def __init__(self, web_content_creator):
        self.creator = web_content_creator
        self.cache = web_content_creator.cache
        self.url_utils = web_content_creator.url_utils
        self.document_creator = web_content_creator.document_creator
        self.semaphore = asyncio.Semaphore(10)
        self._domain_cache = {}
        self._url_tag_selectors = {'a': 'href', 'img': ['src', 'data-src'], 'link': 'href', 'script': 'src'}
        self._content_type_cache = {}

    @lru_cache(maxsize=1000)
    def _clean_url(self, url: str, base_domain: str) -> str:
        """
        Limpa e normaliza URLs com cache.
        Apenas URLs do mesmo domínio são processadas, outras permanecem inalteradas.
        """
        if not url or url.startswith('/'):
            return url
            
        try:
            parsed = urlparse(url)
            
            # Se não tem netloc (domínio), provavelmente é relativo
            if not parsed.netloc:
                return url
                
            url_domain = parsed.netloc.lower()
            
            if base_domain not in self._domain_cache:
                base_domain_parsed = urlparse(base_domain)
                self._domain_cache[base_domain] = base_domain_parsed.netloc.lower() if base_domain_parsed.netloc else base_domain.lower()
            base_netloc = self._domain_cache[base_domain]
            
            # Se não for do mesmo domínio, retorna a URL original sem modificações
            if url_domain != base_netloc:
                return url
                
            # Se for do mesmo domínio, converte para relativo
            path_parts = []
            if parsed.path:
                path_parts.append(parsed.path.lstrip('/'))
            if parsed.query:
                path_parts.append(f"?{parsed.query}")
            if parsed.fragment:
                path_parts.append(f"#{parsed.fragment}")
                
            return f"/{(''.join(path_parts))}"
            
        except Exception as e:
            logger.error(f"Error cleaning URL {url}: {str(e)}")
            return url

    def _clean_img_attributes(self, soup: BeautifulSoup) -> None:
        """Remove atributos desnecessários das tags de imagem"""
        for img in soup.find_all('img'):
            img.attrs = {k: v for k, v in img.attrs.items() if k not in self.IMG_ATTRIBUTES_TO_REMOVE}

    def _clean_content(self, html_content: str) -> str:
        """Limpa o conteúdo HTML de elementos desnecessários"""
        if not html_content:
            return ""
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Verificar se o conteúdo foi selecionado por classe
        is_class_selector = False
        root_element = soup.find()
        
        if root_element and root_element.name == 'div' and 'class' in root_element.attrs:
            root_classes = set(root_element.get('class', []) if isinstance(root_element.get('class'), list) else root_element.get('class', '').split())
            
            for selector in self.CLASS_SELECTORS_SET:
                selector_classes = set(selector.split())
                if selector_classes.issubset(root_classes):
                    is_class_selector = True
                    break
        
        elements_to_remove = []
        
        # Remove date/time patterns
        for date_div in soup.find_all('div', style='font-size:14px;'):
            if self.DATE_PATTERN.search(date_div.text):
                elements_to_remove.append(date_div)
        
        # Remover o título principal apenas se o conteúdo foi encontrado por seletor de classe
        if is_class_selector:
            # Remover o primeiro heading
            first_heading = soup.find(list(self.HEADING_TAGS))
            if first_heading:
                elements_to_remove.append(first_heading)
        
        # Remove all elements marked for removal
        for element in elements_to_remove:
            element.decompose()
        
        # Limpa atributos de imagem
        self._clean_img_attributes(soup)
        
        # Processamen de elementos em uma única passagem
        for element in soup.find_all(['a', 'p', 'div']):
            if element.name == 'a' and 'href' in element.attrs:
                href = element['href']
                if self.EMAIL_HREF_PATTERN.match(href):
                    element['href'] = f'mailto:{href.lstrip("/")}'
                    
            elif element.name == 'p':
                if not element.string:
                    continue
                if self.EMPTY_PARAGRAPH_PATTERN.match(str(element.string)):
                    element.decompose()
                    
            elif element.name == 'div' and element.get('class') and 'margin-top-20' in element.get('class', []):
                element.decompose()
        
        return str(soup)

    def _remove_title_from_content(self, html_content: str) -> str:
        """Remove sempre o primeiro h3 encontrado no início do conteúdo"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            main_div = soup.find('div')
            if not main_div:
                return html_content
                
            first_h3 = main_div.find('h3')
            if first_h3:
                logger.info(f"Removendo primeiro h3: '{first_h3.text.strip()}'")
                first_h3.decompose()
                
                # Isso elimina espaços em branco desnecessários após o título
                if main_div.find('p') and not main_div.find('p').get_text(strip=True):
                    main_div.find('p').decompose()
                    
            return str(soup)
            
        except Exception as e:
            logger.error(f"Erro ao remover título h3: {str(e)}")
            return html_content

    async def _process_url_batch(self, urls_to_process: List[Tuple[Tag, str, str]], base_domain: str, folder_id: int, base_url: str) -> None:
        """Processa um lote de URLs em paralelo com limites de concorrência"""
        tasks = []
        for tag, attr, url in urls_to_process:
            if cached_url := self.cache.get_url(url):
                tag[attr] = cached_url
                continue

            task = self._process_single_url(tag, attr, url, base_domain, folder_id, base_url)
            tasks.append(task)
            
        if tasks:
            # Process in batches with semaphore control
            await asyncio.gather(*tasks)

    async def _process_single_url(self, tag: Tag, attr: str, url: str, base_domain: str, folder_id: int, base_url: str) -> None:
        """Processa URLs individuais com concorrência controlada"""
        async with self.semaphore:
            try:
                is_document = any(doc_path in url for doc_path in self.DOCUMENT_PATHS)
                
                if is_document:
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
        """Processa conteúdo HTML com processamento em lotes para melhor performance"""
        if not html_content:
            return ""

        soup = BeautifulSoup(html_content, 'html.parser')
        base_domain = self.url_utils.extract_domain(base_url)

        self._clean_img_attributes(soup)

        urls_to_process = []
        
        for tag_name, attrs in self._url_tag_selectors.items():
            for tag in soup.find_all(tag_name):
                # Handle both single attribute and lists of attributes
                if isinstance(attrs, list):
                    for attr in attrs:
                        if url := tag.get(attr):
                            urls_to_process.append((tag, attr, url))
                            break  # Only process the first valid attribute
                elif url := tag.get(attrs):
                    urls_to_process.append((tag, attrs, url))

        batch_size = 20
        for i in range(0, len(urls_to_process), batch_size):
            batch = urls_to_process[i:i + batch_size]
            await self._process_url_batch(batch, base_domain, folder_id, base_url)

        return str(soup)

    def _clean_first_div_bootstrap(self, html_content: str) -> str:
        """Remove classes Bootstrap da primeira div"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            first_div = soup.find('div')
            
            if not first_div or 'class' not in first_div.attrs:
                return html_content
            
            classes = first_div.get('class', [])
            if isinstance(classes, str):
                classes = classes.split()
            
            bootstrap_classes = set()
            other_classes = []
            
            for c in classes:
                if self.BOOTSTRAP_PATTERNS.match(c):
                    bootstrap_classes.add(c)
                else:
                    other_classes.append(c)
            
            if bootstrap_classes:
                if other_classes:
                    first_div['class'] = other_classes
                else:
                    del first_div['class']
            
            return str(soup)
            
        except Exception as e:
            logger.error(f"Error cleaning bootstrap classes: {str(e)}")
            return html_content

    def _is_collapsible_content(self, html_content: str) -> bool:
        """Verifica se o conteúdo é colapsável com cache"""
        # Use cache when possible
        content_hash = hash(html_content)
        if content_hash in self._content_type_cache:
            return self._content_type_cache[content_hash]
            
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Use attribute selectors for better performance
            panels = soup.select('div.panel, div.panel-default, div.panel-success')
            if not panels:
                self._content_type_cache[content_hash] = False
                return False
            
            # Use faster selector-based checks
            for panel in panels:
                has_heading = bool(panel.select_one('div.panel-heading'))
                has_collapse = bool(panel.select_one('div.panel-collapse'))
                has_body = bool(panel.select_one('div.panel-body'))
                has_title = bool(panel.select_one('h4.panel-title, h3.panel-title, p.panel-title'))
                
                if (has_heading and (has_collapse or has_body)) or (has_title and has_body):
                    self._content_type_cache[content_hash] = True
                    return True
            
            self._content_type_cache[content_hash] = False
            return False
            
        except Exception as e:
            logger.error(f"Error checking collapsible content: {str(e)}")
            self._content_type_cache[content_hash] = False
            return False

    async def fetch_and_process_content(self, url: str, folder_id: Optional[int] = None) -> Dict[str, any]:
        """Busca e processa conteúdo completo com processamento parale"""
        try:
            # Busca conteúdo
            html_content = await self.creator.fetch_content(url)
            if not html_content:
                return {"success": False, "error": "No content found"}

            # Executa tarefas em paralelo para melhor performance
            tasks = [
                self.process_content(html_content, url, folder_id),
                self._is_collapsible_content_async(html_content)
            ]
            
            processed_content, is_fully_collapsible = await asyncio.gather(*tasks)
            
            if not processed_content:
                return {"success": False, "error": "Failed to process content"}
            
            # Limpa classes bootstrap
            cleaned_content = self._clean_first_div_bootstrap(processed_content)
            
            # Remoção do título duplicado
            cleaned_content = self._remove_title_from_content(cleaned_content)
            
            # Verificação de mixed_content
            has_some_collapsible = False
            if not is_fully_collapsible:
                soup = BeautifulSoup(cleaned_content, 'html.parser')
                
                # Extract div elements more efficiently using CSS selector
                elements = soup.select('div', limit=30)
                
                # Process in larger chunks with optimized batch size
                chunk_size = 15
                chunks = [elements[i:i + chunk_size] for i in range(0, len(elements), chunk_size)]
                
                # Check all chunks in parallel
                results = await asyncio.gather(*[
                    self._check_collapsible_elements(chunk) for chunk in chunks
                ])
                has_some_collapsible = any(results)

            return {
                "success": True,
                "content": cleaned_content,
                "is_collapsible": is_fully_collapsible,
                "has_mixed_content": has_some_collapsible and not is_fully_collapsible
            }

        except Exception as e:
            logger.error(f"Error processing content from {url}: {str(e)}")
            return {"success": False, "error": str(e)}
        
    async def _is_collapsible_content_async(self, html_content: str) -> bool:
        """Versão assíncrona do verificador de conteúdo colapsável"""
        # Offload to thread pool for better performance on CPU-bound task
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._is_collapsible_content, html_content)

    async def _check_collapsible_elements(self, elements: List[Tag]) -> bool:
        """Verifica se algum elemento é colapsável"""
        # Use a more efficient approach for checking multiple elements
        content_to_check = [str(elem) for elem in elements]
        
        # Run checks in parallel for better performance
        tasks = [
            self._is_collapsible_content_async(content) for content in content_to_check
        ]
        results = await asyncio.gather(*tasks)
        
        return any(results)