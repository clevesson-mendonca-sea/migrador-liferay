import logging
from bs4 import BeautifulSoup, Tag
import re
from functools import lru_cache
from typing import List, Optional, Dict, Set, Tuple, Any, Union
from urllib.parse import urlparse
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
    
    # Padrões de data aprimorados
    DATE_PATTERNS = [
        # Padrão completo com atualização
        re.compile(r'\d{1,2}/\d{1,2}/\d{2,4}\s+às\s+\d{1,2}h\d{1,2}(?:\s+-\s+Atualizado\s+em\s+\d{1,2}/\d{1,2}/\d{2,4}\s+às\s+\d{1,2}h\d{1,2})?'),
        # Padrão simples de data com hora
        re.compile(r'\d{1,2}/\d{1,2}/\d{2,4}\s+às\s+\d{1,2}h\d{1,2}'),
        # Padrão de atualização/publicação
        re.compile(r'(?:Atualizado|Publicado)\s+em\s+\d{1,2}/\d{1,2}/\d{2,4}')
    ]
    
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
    
    # Padrões de colapsáveis
    COLLAPSE_PANEL_PATTERNS = [
        {'panel': ['panel', 'panel-success', 'panel-default'], 'heading': 'panel-heading', 'body': 'panel-body'},
        {'button': ['btn', 'btn-primary'], 'collapse': 'collapse', 'container': 'well'}
    ]

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
            # Backup do src original para garantir que não se perca
            orig_src = img.get('src')
            
            # Remover apenas atributos desnecessários
            img.attrs = {k: v for k, v in img.attrs.items() if k not in self.IMG_ATTRIBUTES_TO_REMOVE}
            
            # Garantir que o src está presente e não foi alterado
            if orig_src and 'src' not in img.attrs:
                img['src'] = orig_src
                logger.debug(f"Restaurado atributo src: {orig_src}")

    def _clean_content(self, html_content: str) -> str:
        """Limpa o conteúdo HTML de elementos desnecessários preservando imagens"""
        if not html_content:
            return ""
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Salvar todas as tags de imagem e seus containers antes da limpeza
        images_data = []
        for img in soup.find_all('img'):
            # Salvar src original
            src = img.get('src', '')
            
            # Encontrar o container de parágrafo mais próximo
            container = img.parent
            while container and container.name != 'p':
                container = container.parent
                
            if container and container.name == 'p':
                # Salvar a estrutura completa: p > a > img
                if img.parent and img.parent.name == 'a':
                    a_tag = img.parent
                    a_href = a_tag.get('href', '')
                    # Se o parágrafo contém apenas este link com imagem, salvar toda a estrutura
                    if len(container.contents) == 1 and container.contents[0] == a_tag:
                        images_data.append(('p_with_a_and_img', container, a_href, src))
                    else:
                        images_data.append(('img_in_a', img, a_href, src))
                else:
                    images_data.append(('img', img, None, src))
            else:
                images_data.append(('img', img, None, src))
                
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
        
        # 1. Remover divs com datas
        date_divs = soup.find_all('div', style=lambda s: s and 'font-size:14px' in s)
        for div in date_divs:
            text = div.get_text(strip=True)
            for pattern in self.DATE_PATTERNS:
                if pattern.search(text):
                    logger.info(f"Removendo div com data: '{text}'")
                    elements_to_remove.append(div)
                    break
        
        # 2. Verificar outras datas em parágrafos e spans
        for element in soup.find_all(['p', 'span']):
            if not element.string:
                continue
                
            text = element.string.strip()
            for pattern in self.DATE_PATTERNS:
                if pattern.search(text) and len(text) < 50:
                    # Verificar se este elemento contém uma imagem antes de remover
                    contains_img = bool(element.find('img'))
                    if not contains_img:
                        logger.info(f"Removendo elemento com data: '{text}'")
                        elements_to_remove.append(element)
                    break
        
        # Remover o título principal apenas se o conteúdo foi encontrado por seletor de classe
        if is_class_selector:
            # Remover o primeiro heading - verificar se não contém imagem
            first_heading = soup.find(list(self.HEADING_TAGS))
            if first_heading and not first_heading.find('img'):
                elements_to_remove.append(first_heading)
        
        # Remove margin-top divs - verificar se não contêm imagens
        for div in soup.find_all('div', class_=lambda c: c and 'margin-top' in c):
            if not div.find('img'):
                elements_to_remove.append(div)
        
        # Remover elementos marcados (que não contenham imagens)
        for element in elements_to_remove:
            if not element.find('img'):  # Verificação adicional
                element.decompose()
            else:
                logger.info(f"Não removendo elemento com imagem: {element.name}")
        
        # Limpar atributos de imagem sem remover as próprias imagens
        self._clean_img_attributes(soup)
        
        # Processar elementos em uma única passagem (verificando imagens)
        for element in soup.find_all(['a', 'p']):
            if element.name == 'a' and 'href' in element.attrs and not element.find('img'):
                href = element['href']
                if self.EMAIL_HREF_PATTERN.match(href):
                    element['href'] = f'mailto:{href.lstrip("/")}'
                    
            elif element.name == 'p' and not element.find('img'):
                if not element.string:
                    continue
                if self.EMPTY_PARAGRAPH_PATTERN.match(str(element.string)):
                    element.decompose()
        
        # Verificar se alguma imagem foi perdida e restaurá-la
        current_images = set(img.get('src', '') for img in soup.find_all('img'))
        for img_type, element, href, src in images_data:
            if src and src not in current_images:
                logger.info(f"Restaurando imagem perdida: {src}")
                if img_type == 'img':
                    # Criar nova tag img
                    new_img = soup.new_tag('img', src=src)
                    soup.append(new_img)
                elif img_type == 'img_in_a' and href:
                    # Criar estrutura a > img
                    new_a = soup.new_tag('a', href=href)
                    new_img = soup.new_tag('img', src=src)
                    new_a.append(new_img)
                    soup.append(new_a)
                elif img_type == 'p_with_a_and_img' and href:
                    # Criar estrutura p > a > img
                    new_p = soup.new_tag('p')
                    new_a = soup.new_tag('a', href=href)
                    new_img = soup.new_tag('img', src=src)
                    new_a.append(new_img)
                    new_p.append(new_a)
                    soup.append(new_p)
        
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
                # Verificar se o h3 contém uma imagem antes de remover
                if not first_h3.find('img'):
                    logger.info(f"Removendo primeiro h3: '{first_h3.text.strip()}'")
                    first_h3.decompose()
                    
                    # Isso elimina espaços em branco desnecessários após o título
                    if main_div.find('p') and not main_div.find('p').get_text(strip=True) and not main_div.find('p').find('img'):
                        main_div.find('p').decompose()
                else:
                    logger.info(f"Não removendo h3 '{first_h3.text.strip()}' porque contém imagem")
                    
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
        """Processa URLs individuais com melhor tratamento de erros"""
        async with self.semaphore:
            try:
                # 1. Validação e limpeza básica da URL
                if not url or url.isspace():
                    logger.warning(f"URL vazia ou inválida encontrada")
                    self.cache.mark_failed(url)
                    return

                # 2. Limpa a URL antes de processá-la
                original_url = url
                cleaned_url = self._clean_url_before_processing(url)
                
                if not cleaned_url:
                    logger.warning(f"URL inválida após limpeza: {original_url}")
                    self.cache.mark_failed(original_url)
                    return

                # 3. Verifica se é um documento
                is_document = any(doc_path in cleaned_url.lower() for doc_path in self.DOCUMENT_PATHS)
                
                # 4. Processa documentos
                if is_document:
                    try:
                        # Constrói URL completa se for relativa
                        full_url = cleaned_url
                        if not cleaned_url.startswith(('http://', 'https://')):
                            full_url = f"{base_domain.rstrip('/')}/{cleaned_url.lstrip('/')}"
                        
                        # Validação adicional da URL
                        parsed_url = urlparse(full_url)
                        if not all([parsed_url.scheme, parsed_url.netloc]):
                            logger.error(f"URL mal formada após limpeza: {full_url}")
                            self.cache.mark_failed(original_url)
                            tag[attr] = original_url  # Mantém a URL original em caso de erro
                            return

                        # Verificar se a URL é de imagem para log específico
                        is_image = any(full_url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp'])
                        if is_image:
                            logger.info(f"Processando imagem: {full_url}")

                        # Use a função existente para migrar o documento
                        migrated_url = await self.creator._retry_operation(
                            self.document_creator.migrate_document,
                            doc_url=full_url,
                            folder_id=folder_id,
                            page_url=base_url
                        )

                        if migrated_url:
                            relative_url = self._clean_url(migrated_url, base_domain)
                            self.cache.add_url(original_url, relative_url)
                            tag[attr] = relative_url
                            
                            if is_image:
                                logger.info(f"Imagem migrada com sucesso: {original_url} -> {relative_url}")
                        else:
                            logger.warning(f"Falha na migração do documento: {full_url}")
                            self.cache.mark_failed(original_url)
                            tag[attr] = original_url  # Mantém a URL original em caso de falha
                    except Exception as doc_error:
                        logger.error(f"Erro ao processar documento {full_url}: {str(doc_error)}")
                        self.cache.mark_failed(original_url)
                        tag[attr] = original_url  # Mantém a URL original em caso de exceção
                else:
                    # Processa outras URLs mantendo externas como absolutas
                    try:
                        cleaned_url = self._clean_url(cleaned_url, base_domain)
                        tag[attr] = cleaned_url
                    except Exception as url_error:
                        logger.error(f"Erro ao limpar URL {cleaned_url}: {str(url_error)}")
                        self.cache.mark_failed(original_url)
                        tag[attr] = original_url  # Mantém a URL original em caso de exceção

            except Exception as e:
                logger.error(f"Erro crítico processando URL {url}: {str(e)}")
                self.cache.mark_failed(url)
                tag[attr] = url  # Mantém a URL original em caso de erro crítico

    def _clean_url_before_processing(self, url: str) -> str:
        """Limpa e normaliza a URL antes de processá-la"""
        if not url:
            return ""
            
        # Remove aspas HTML (&quot;)
        url = url.replace('&quot;', '')
        
        # Remove aspas extras
        url = url.strip('"\'')
        
        # Corrige URLs com espaços e múltiplos protocolos
        if ' http://' in url or ' https://' in url:
            # Pega apenas a última URL se houver múltiplas
            parts = re.split(r'\s+(https?://)', url)
            if len(parts) > 2:  # Encontrou múltiplos protocolos
                url = parts[-2] + parts[-1]  # Pega o último protocolo e caminho
            else:
                # Simples caso de espaço antes do protocolo
                url = re.sub(r'\s+(https?://)', r'\1', url)
        
        # Normaliza barras invertidas
        url = url.replace('\\', '/')
        
        return url.strip()

    async def process_content(self, html_content: str, base_url: str, folder_id: Optional[int] = None) -> str:
        """Processa conteúdo HTML com processamento em lotes para melhor performance"""
        if not html_content:
            return ""

        soup = BeautifulSoup(html_content, 'html.parser')
        base_domain = self.url_utils.extract_domain(base_url)

        # Contagem de imagens antes do processamento
        img_count_before = len(soup.find_all('img'))
        logger.info(f"Processando conteúdo com {img_count_before} imagens")

        self._clean_img_attributes(soup)

        urls_to_process = []
        
        for tag_name, attrs in self._url_tag_selectors.items():
            for tag in soup.find_all(tag_name):
                # Handle both single attribute and lists of attributes
                if isinstance(attrs, list):
                    for attr in attrs:
                        if url := tag.get(attr):
                            # Log especial para imagens
                            if tag_name == 'img' and attr == 'src':
                                logger.debug(f"Adicionando imagem para processamento: {url}")
                            urls_to_process.append((tag, attr, url))
                            break  # Only process the first valid attribute
                elif url := tag.get(attrs):
                    # Log especial para links com imagens
                    if tag_name == 'a' and tag.find('img'):
                        logger.debug(f"Adicionando link com imagem para processamento: {url}")
                    urls_to_process.append((tag, attrs, url))

        # Log especial para processamento de imagens
        image_urls = [(tag, attr, url) for tag, attr, url in urls_to_process if 
                    (isinstance(tag, Tag) and tag.name == 'img' and attr == 'src') or
                    (any(url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp']))]
        
        if image_urls:
            logger.info(f"Encontradas {len(image_urls)} URLs de imagens para processamento")

        batch_size = 20
        for i in range(0, len(urls_to_process), batch_size):
            batch = urls_to_process[i:i + batch_size]
            await self._process_url_batch(batch, base_domain, folder_id, base_url)

        # Contagem de imagens após o processamento
        img_count_after = len(soup.find_all('img'))
        if img_count_before != img_count_after:
            logger.warning(f"Número de imagens mudou durante o processamento: {img_count_before} -> {img_count_after}")

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

    def _detect_collapsible_type(self, html_content: str) -> str:
        """
        Detecta o tipo de conteúdo colapsável
        Retorna: 'panel', 'button', 'mixed', 'none'
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Verifica padrão de painel tradicional (panel-default, panel-success)
            has_panel = bool(soup.select('div.panel, div.panel-default, div.panel-success'))
            
            # Verifica padrão de botão com collapse
            has_button_collapse = False
            buttons = soup.select('button.btn.btn-primary[data-toggle="collapse"], button.btn[data-toggle="collapse"]')
            
            if buttons:
                for button in buttons:
                    target_id = button.get('data-target', '').strip('#')
                    if target_id and soup.select(f'div.collapse#{target_id}, div.collapse.in#{target_id}'):
                        has_button_collapse = True
                        break
            
            # Determina o tipo
            if has_panel and has_button_collapse:
                return 'mixed'
            elif has_panel:
                return 'panel'
            elif has_button_collapse:
                return 'button'
            else:
                return 'none'
                
        except Exception as e:
            logger.error(f"Error detecting collapsible type: {str(e)}")
            return 'none'

    def _is_collapsible_content(self, html_content: str) -> bool:
        """Verifica se o conteúdo é colapsável com cache"""
        # Use cache when possible
        content_hash = hash(html_content)
        if content_hash in self._content_type_cache:
            return self._content_type_cache[content_hash]
            
        try:
            collapsible_type = self._detect_collapsible_type(html_content)
            result = collapsible_type != 'none'
            
            self._content_type_cache[content_hash] = result
            return result
            
        except Exception as e:
            logger.error(f"Error checking collapsible content: {str(e)}")
            self._content_type_cache[content_hash] = False
            return False

    async def fetch_and_process_content(self, url: str, folder_id: Optional[int] = None) -> Dict[str, any]:
        """Busca e processa conteúdo completo com processamento paralelo"""
        try:
            # Busca conteúdo
            html_content = await self.creator.fetch_content(url)
            if not html_content:
                return {"success": False, "error": "No content found"}

            logger.info(f"Processando conteúdo da URL: {url}")
            
            # Contar imagens no conteúdo original
            soup_original = BeautifulSoup(html_content, 'html.parser')
            img_count_original = len(soup_original.find_all('img'))
            if img_count_original > 0:
                logger.info(f"Conteúdo original contém {img_count_original} imagens")
                
                # Listar todas as imagens para debug
                for img in soup_original.find_all('img'):
                    logger.debug(f"Imagem original: {img.get('src', 'sem-src')}")

            # Processar conteúdo
            processed_content = await self.process_content(html_content, url, folder_id)
            
            if not processed_content:
                return {"success": False, "error": "Failed to process content"}
            
            # Verificar imagens após processamento inicial
            soup_processed = BeautifulSoup(processed_content, 'html.parser')
            img_count_processed = len(soup_processed.find_all('img'))
            if img_count_processed != img_count_original:
                logger.warning(f"Após processamento inicial: {img_count_processed}/{img_count_original} imagens")
            
            # Limpar classes Bootstrap
            cleaned_content = self._clean_first_div_bootstrap(processed_content)
            
            # Verificar imagens após limpeza de Bootstrap
            soup_cleaned = BeautifulSoup(cleaned_content, 'html.parser')
            img_count_cleaned = len(soup_cleaned.find_all('img'))
            if img_count_cleaned != img_count_processed:
                logger.warning(f"Após limpeza de Bootstrap: {img_count_cleaned}/{img_count_processed} imagens")
            
            # Limpar conteúdo (remoção de datas, etc.)
            cleaned_content = self._clean_content(cleaned_content)
            
            # Verificar imagens após limpeza de conteúdo
            soup_after_clean = BeautifulSoup(cleaned_content, 'html.parser')
            img_count_after_clean = len(soup_after_clean.find_all('img'))
            if img_count_after_clean != img_count_cleaned:
                logger.warning(f"Após limpeza de conteúdo: {img_count_after_clean}/{img_count_cleaned} imagens")
            
            # Remover título
            final_content = self._remove_title_from_content(cleaned_content)
            
            # Verificar imagens após remoção de título
            soup_final = BeautifulSoup(final_content, 'html.parser')
            img_count_final = len(soup_final.find_all('img'))
            if img_count_final != img_count_after_clean:
                logger.warning(f"Após remoção de título: {img_count_final}/{img_count_after_clean} imagens")
            
            # Verificar se alguma imagem foi perdida completamente
            if img_count_original > 0 and img_count_final < img_count_original:
                logger.warning(f"ALERTA: {img_count_original - img_count_final} imagens foram perdidas durante o processamento")
                
                # Tentar recuperar imagens perdidas
                original_imgs = soup_original.find_all('img')
                final_imgs = soup_final.find_all('img')
                final_srcs = [img.get('src', '') for img in final_imgs]
                
                for img in original_imgs:
                    original_src = img.get('src', '')
                    if original_src and original_src not in final_srcs:
                        logger.info(f"Tentando recuperar imagem perdida: {original_src}")
                        
                        # Encontrar a URL migrada no cache
                        migrated_src = self.cache.get_url(original_src)
                        if migrated_src:
                            # Criar uma nova tag de imagem com a URL migrada
                            container_p = soup_final.new_tag('p')
                            new_img = soup_final.new_tag('img', src=migrated_src)
                            
                            # Copiar atributos importantes da imagem original
                            for attr in ['alt', 'width', 'height', 'class', 'style']:
                                if attr_value := img.get(attr):
                                    new_img[attr] = attr_value
                            
                            # Adicionar a imagem recuperada ao conteúdo
                            container_p.append(new_img)
                            if soup_final.body:
                                soup_final.body.append(container_p)
                            else:
                                soup_final.append(container_p)
                            
                            logger.info(f"Imagem recuperada: {original_src} -> {migrated_src}")
                
                # Usar o HTML atualizado com imagens recuperadas
                if img_count_original > img_count_final:
                    final_content = str(soup_final)
            
            # Detectar tipo de colapsável
            collapsible_type = self._detect_collapsible_type(final_content)
            
            return {
                "success": True,
                "content": final_content,
                "is_collapsible": collapsible_type in ('panel', 'button'),
                "has_mixed_content": collapsible_type == 'mixed',
                "collapsible_type": collapsible_type
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