import logging
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse
import os
from typing import Optional, Dict, List, Tuple
import traceback
import json
import difflib

logger = logging.getLogger(__name__)

class ContentValidator:
    def __init__(self, config):
        self.config = config
        self.session = None
        self.logs_dir = "logs"
        os.makedirs(self.logs_dir, exist_ok=True)
        
        # Arquivos de log
        self.validation_log_file = os.path.join(self.logs_dir, "validation_results.txt")
        self.content_mismatch_file = os.path.join(self.logs_dir, "content_mismatch_errors.txt")
        self.menu_mismatch_file = os.path.join(self.logs_dir, "menu_mismatch_errors.txt")
        self.collapsible_mismatch_file = os.path.join(self.logs_dir, "collapsible_mismatch_errors.txt")
        self.validation_json_file = os.path.join(self.logs_dir, "validation_results.json")
        
        self.errors: Dict[str, List[str]] = {}
            
    async def initialize_session(self):
        """Initialize HTTP session"""
        if self.session:
            await self.session.close()
            
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False)
        )

    async def close(self):
        """Close session"""
        if self.session:
            await self.session.close()

    def _log_validation_error(self, source_url: str, error_type: str, details: str, migrated_url: str = None):
        """
        Registra erros de validação
        Args:
            source_url: URL original
            error_type: Tipo do erro
            details: Detalhes do erro
            migrated_url: URL migrada (opcional)
        """
        if source_url not in self.errors:
            self.errors[source_url] = []
        
        self.errors[source_url].append(f"{error_type}: {details}")
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_entry = (
            f"\n=== Validation Error: {timestamp} ===\n"
            f"URL Original: {source_url}\n"
        )
        
        if migrated_url:
            error_entry += f"URL Migrada: {migrated_url}\n"
            
        error_entry += (
            f"Type: {error_type}\n"
            f"Details: {details}\n"
            f"{'=' * 50}\n"
        )
        
        # Decide qual arquivo usar baseado no tipo de erro
        if "Collapsible" in error_type:
            filename = self.collapsible_mismatch_file
        elif "Conteúdo não corresponde ao original" in details:
            filename = self.content_mismatch_file
        elif "Menu não corresponde ao original" in details:
            filename = self.menu_mismatch_file
        else:
            filename = self.validation_log_file
            
        try:
            with open(filename, "a", encoding="utf-8") as f:
                f.write(error_entry)
        except Exception as e:
            logger.error(f"Erro ao salvar log de validação: {str(e)}")

        # Log em JSON
        try:
            error_json = {
                "timestamp": timestamp,
                "url_original": source_url,
                "url_migrada": migrated_url,
                "error_type": error_type,
                "details": details
            }
            
            existing_errors = []
            if os.path.exists(self.validation_json_file):
                with open(self.validation_json_file, "r", encoding="utf-8") as f:
                    existing_errors = json.load(f)
                    
            existing_errors.append(error_json)
            
            with open(self.validation_json_file, "w", encoding="utf-8") as f:
                json.dump(existing_errors, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Erro ao salvar log JSON: {str(e)}")
               
    async def fetch_content(self, url: str) -> Optional[str]:
        """Busca o conteúdo da página"""
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                logger.error(f"Erro ao buscar {url}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Erro ao buscar {url}: {str(e)}")
            return None
        
    def normalize_content(self, html: str) -> str:
        """Normaliza o conteúdo removendo elementos não relevantes"""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove tags não desejadas
        for tag in soup.find_all(['script', 'style', 'audio', 'video', 'iframe']):
            tag.decompose()
            
        # Remove atributos de estilo e classe
        for tag in soup.find_all(True):
            for attr in ['style', 'class', 'id']:
                if attr in tag.attrs:
                    del tag.attrs[attr]
                    
        # Remove atributos de imagem que podem variar
        for img in soup.find_all('img'):
            for attr in ['width', 'height', 'loading', 'decoding']:
                if attr in img.attrs:
                    del img.attrs[attr]
                    
        # Remove espaços extras e quebras de linha
        text = ' '.join(soup.get_text().split())
        return text

    def _normalize_text(self, text: str) -> str:
        """
        Normalizes text for comparison by:
        - Removing extra whitespace
        - Removing special characters
        - Standardizing text
        """
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Remove special characters
        text = text.replace('\xa0', ' ')  # non-breaking space
        text = text.replace('\u200b', '')  # zero-width space
        text = text.replace('\t', ' ')     # tabs
        
        # Make lowercase for case-insensitive comparison
        text = text.lower()
        
        return text.strip()

    def check_images(self, html: str, url: str) -> bool:
        """Verifica se as imagens foram migradas para /documents"""
        soup = BeautifulSoup(html, 'html.parser')
        all_ok = True
        
        for img in soup.find_all('img'):
            src = img.get('src', '')
            if not src.startswith('/documents'):
                self._log_validation_error(
                    url,
                    "Image not migrated",
                    f"Image source not in /documents: {src}"
                )
                all_ok = False
                
        return all_ok

    def _extract_collapsible_title(self, element: BeautifulSoup, is_original: bool = True) -> str:
        """
        Extracts title from collapsible element handling both formats
        Returns cleaned title text
        """
        title_text = ""
        
        if is_original:
            # Para o formato original
            strong = element.find('strong')
            if strong:
                title_text = strong.get_text(strip=True)
            else:
                p_tag = element.find('p')
                if p_tag:
                    title_text = p_tag.get_text(strip=True)
                else:
                    title_text = element.get_text(strip=True)
        else:
            # Para o formato migrado
            span = element.find('span', class_='panel-title')
            if span:
                title_text = span.get_text(strip=True)
            else:
                title_text = element.get_text(strip=True)
        
        # Log para debug
        logger.debug(f"Extracted title: '{title_text}' from {'original' if is_original else 'migrated'} format")
        
        return title_text.strip()

    def _normalize_title(self, title: str) -> str:
        """
        Normalizes title text for comparison by removing special chars and extra whitespace
        """
        # Remove caracteres especiais e espaços extras
        title = ' '.join(title.split())
        title = title.replace('\xa0', ' ')  # non-breaking space
        title = title.replace('\u200b', '')  # zero-width space
        title = title.replace('\t', ' ')     # tabs
        
        # Remove pontuação e caracteres especiais
        title = ''.join(c for c in title if c.isalnum() or c.isspace())
        
        # Converte para minúsculas
        title = title.lower()
        
        # Remove palavras de teste
        for prefix in ['test', 'testing', '(test)', '[test]']:
            title = title.replace(prefix, '')
        
        # Log para debug
        logger.debug(f"Normalized title: '{title}'")
        
        return title.strip()

    def _compare_collapsibles(self, original_soup: BeautifulSoup, migrated_soup: BeautifulSoup) -> Optional[str]:
        """
        Compares collapsible sections between original and migrated pages
        Returns error message if differences found, None if identical
        """
        original_sections = []
        migrated_sections = []
        
        # Extrai seções originais
        for heading in original_soup.find_all('div', class_='panel-heading'):
            title = self._extract_collapsible_title(heading, True)
            content_div = heading.find_next('div', class_='panel-collapse')
            if content_div and content_div.find('div', class_='panel-body'):
                content = self._normalize_text(content_div.find('div', class_='panel-body').get_text())
                original_sections.append({
                    'title': title,
                    'content': content
                })
                logger.debug(f"Found original section with title: '{title}'")
        
        # Extrai seções migradas
        for button in migrated_soup.find_all('button', class_='btn-unstyled'):
            title = self._extract_collapsible_title(button, False)
            content_id = button.get('aria-controls')
            if content_id:
                content_div = migrated_soup.find('div', id=content_id)
                if content_div and content_div.find('div', class_='panel-body'):
                    content = self._normalize_text(content_div.find('div', class_='panel-body').get_text())
                    migrated_sections.append({
                        'title': title,
                        'content': content
                    })
                    logger.debug(f"Found migrated section with title: '{title}'")
        
        # Comparação
        if len(original_sections) != len(migrated_sections):
            return (f"Diferença no número de seções colapsáveis: "
                    f"Original ({len(original_sections)}) vs Migrado ({len(migrated_sections)})")
        
        differences = []
        for i, (orig, mig) in enumerate(zip(original_sections, migrated_sections)):
            # Normaliza e compara títulos
            orig_title = self._normalize_title(orig['title'])
            mig_title = self._normalize_title(mig['title'])
            
            logger.debug(f"Comparing titles:\nOriginal: '{orig_title}'\nMigrated: '{mig_title}'")
            
            if orig_title != mig_title:
                differences.append(
                    f"Seção {i+1}: Título diferente\n"
                    f"Original: '{orig['title']}'\n"
                    f"Migrado: '{mig['title']}'"
                )
            
            # Compara conteúdo
            orig_content = self._normalize_text(orig['content'])
            mig_content = self._normalize_text(mig['content'])
            
            if orig_content != mig_content:
                diff = self._get_content_diff(orig_content, mig_content)
                differences.append(
                    f"Seção {i+1} ({orig['title']}): Conteúdo diferente\n{diff}"
                )
        
        if differences:
            return "Diferenças nos colapsáveis:\n" + "\n".join(differences)
        
        return None
    
    def _extract_collapsible_content(self, soup: BeautifulSoup, is_original: bool = True) -> List[dict]:
        """
        Extracts content from collapsible sections based on the format (original/migrated)
        Returns a list of dictionaries with title and content for each section
        """
        collapsibles = []
        
        if is_original:
            # Original format - Bootstrap panels
            panels = soup.find_all('div', class_='panel-default')
            if not panels:
                # Try alternative format
                panels = []
                headings = soup.find_all('div', class_='panel-heading')
                for heading in headings:
                    panel = heading.find_parent('div', class_='panel') or heading.parent
                    if panel:
                        panels.append(panel)
            
            for panel in panels:
                # Get heading div
                heading = panel.find('div', class_='panel-heading')
                if not heading:
                    continue
                    
                # Extract title
                title = self._extract_collapsible_title(heading, True)
                logger.debug(f"Original title extracted: {title}")  # Debug log
                
                # Get content section
                content_div = panel.find('div', class_='panel-collapse')
                if content_div and content_div.find('div', class_='panel-body'):
                    body_div = content_div.find('div', class_='panel-body')
                    
                    # Extract content, handling tables if present
                    content = ""
                    if body_div:
                        table = body_div.find('table')
                        if table:
                            content = table.get_text(strip=True)
                        else:
                            content = body_div.get_text(strip=True)
                    
                    collapsibles.append({
                        'title': title,
                        'content': content
                    })
        else:
            # Migrated format - New accordion structure
            buttons = soup.find_all('button', class_='btn-unstyled')
            for button in buttons:
                # Extract title
                title = self._extract_collapsible_title(button, False)
                logger.debug(f"Migrated title extracted: {title}")  # Debug log
                
                # Get content div using aria-controls
                content_id = button.get('aria-controls')
                if content_id:
                    content_div = soup.find('div', id=content_id)
                    if content_div and content_div.find('div', class_='panel-body'):
                        body_div = content_div.find('div', class_='panel-body')
                        
                        # Extract content, handling tables if present
                        content = ""
                        if body_div:
                            table = body_div.find('table')
                            if table:
                                content = table.get_text(strip=True)
                            else:
                                content = body_div.get_text(strip=True)
                                
                        collapsibles.append({
                            'title': title,
                            'content': content
                        })
        
        return collapsibles

    def _get_content_diff(self, original: str, migrated: str) -> str:
        """
        Gets detailed difference between two text contents
        Returns formatted string showing changes
        """
        # Split into words for more meaningful comparison
        orig_words = original.split()
        mig_words = migrated.split()
        
        # Use SequenceMatcher for detailed comparison
        differ = difflib.SequenceMatcher(None, orig_words, mig_words)
        
        differences = []
        for tag, i1, i2, j1, j2 in differ.get_opcodes():
            if tag != 'equal':
                orig = ' '.join(orig_words[i1:i2])
                mig = ' '.join(mig_words[j1:j2])
                
                if tag == 'delete':
                    differences.append(f"Removido: '{orig}'")
                elif tag == 'insert':
                    differences.append(f"Adicionado: '{mig}'")
                elif tag == 'replace':
                    differences.append(f"Alterado de '{orig}' para '{mig}'")
        
        return "\n".join(differences) if differences else "Conteúdos diferentes (diferenças específicas não identificadas)"

    def check_relative_links(self, html: str, url: str, base_domain: str) -> bool:
        """Verifica se os links estão relativos"""
        soup = BeautifulSoup(html, 'html.parser')
        all_ok = True
        
        for a in soup.find_all('a'):
            href = a.get('href', '')
            if href and href.startswith(('http://', 'https://')):
                parsed_url = urlparse(href)
                if parsed_url.netloc == base_domain:
                    self._log_validation_error(
                        url,
                        "Absolute link",
                        f"Link should be relative: {href}"
                    )
                    all_ok = False
                
        return all_ok

    def _extract_menu_links(self, soup: BeautifulSoup, is_original: bool = True) -> List[Tuple[str, str]]:
        """
        Extracts links from the lateral menu while properly handling hidden elements
        Returns list of (text, href) tuples for visible menu items
        """
        menu_class = '.menu-lateral-flutuante' if is_original else '.list-menu'
        menu = soup.select_one(menu_class)
        if not menu:
            return []
        
        links = []
        for li in menu.find_all('li', recursive=True):
            # Explicitly check style attribute for display:none
            if li.has_attr('style') and 'display:none' in li['style'].lower().replace(' ', ''):
                continue
                
            # Find link within this li if it's visible
            link = li.find('a')
            if link and not self._is_element_hidden(li):
                text = link.get_text(strip=True)
                href = link.get('href', '')
                if text and href:
                    links.append((text, href))
        
        return links

    def _is_element_hidden(self, element) -> bool:
        """
        Checks if an element or any of its parents have display:none
        Returns True if the element is hidden
        """
        current = element
        
        while current and not current.has_attr('class') or 'menu-lateral-flutuante' not in current.get('class', []):
            if current.has_attr('style'):
                style = current['style'].lower().replace(' ', '')
                if 'display:none' in style:
                    return True
                
            current = current.parent
            
        return False

    def _normalize_url(self, url: str) -> str:
        """
        Normalizes URLs for comparison by:
        1. Removing trailing slashes
        2. Removing domain and protocol
        3. Removing 'w/' prefix if present
        """
        # Remove trailing slash
        url = url.rstrip('/')
        
        # Parse URL to extract path
        parsed = urlparse(url)
        path = parsed.path
        
        # Remove 'w/' prefix if present
        if path.startswith('/w/'):
            path = path.replace('/w/', '/', 1)
            
        # Remove any double slashes
        while '//' in path:
            path = path.replace('//', '/')
            
        return path

    def _compare_menus(self, original_soup: BeautifulSoup, migrated_soup: BeautifulSoup) -> Optional[str]:
        """
        Compares lateral menus and returns details of differences
        Only compares visible menu items
        """
        original_links = self._extract_menu_links(original_soup, True)
        migrated_links = self._extract_menu_links(migrated_soup, False)
        
        if not original_links and not migrated_links:
            return None
            
        if len(original_links) != len(migrated_links):
            visible_orig = len(original_links)
            visible_mig = len(migrated_links)
            return (f"Diferença no número de links visíveis: "
                    f"Original ({visible_orig}) vs Migrado ({visible_mig})")
            
        differences = []
        for i, ((orig_text, orig_href), (mig_text, mig_href)) in enumerate(zip(original_links, migrated_links)):
            if orig_text != mig_text:
                differences.append(
                    f"Link {i+1}: Texto diferente\n"
                    f"Original: '{orig_text}'\n"
                    f"Migrado: '{mig_text}'"
                )
            
            # Compare normalized paths only
            orig_path = self._normalize_url(orig_href)
            mig_path = self._normalize_url(mig_href)
            
            if orig_path != mig_path:
                # Skip special cases like organogram
                if ('organograma' in orig_path and 'organograma' in mig_path) or \
                   ('agenda-do-chefe' in orig_path and 'agenda-do-chefe' in mig_path):
                    continue
                    
                differences.append(
                    f"Link {i+1}: Caminhos diferentes após normalização\n"
                    f"Original: '{orig_path}'\n"
                    f"Migrado: '{mig_path}'"
                )
                    
        if differences:
            return "Diferenças no menu:\n" + "\n".join(differences)
            
        return None

    async def validate_page(self, source_url: str, destination_url: str, title: str) -> bool:
        """Valida o conteúdo entre a página original e a migrada"""
        try:
            logger.info(f"Validando {title}")
            logger.info(f"Original: {source_url}")
            logger.info(f"Migrada: {destination_url}")

            # Busca conteúdo
            original_html = await self.fetch_content(source_url)
            if not original_html:
                self._log_validation_error(source_url, "Fetch Error", 
                    "Não foi possível buscar página original", destination_url)
                return False

            migrated_html = await self.fetch_content(destination_url)
            if not migrated_html:
                self._log_validation_error(source_url, "Fetch Error", 
                    "Não foi possível buscar página migrada", destination_url)
                return False

            # Parse HTML
            original_soup = BeautifulSoup(original_html, 'html.parser')
            migrated_soup = BeautifulSoup(migrated_html, 'html.parser')

            # Encontra os contêineres principais
            original_content = (
                original_soup.select_one('#conteudo') or 
                original_soup.select_one('.col-md-8.col-md-offset-1')
            )
            migrated_content = migrated_soup.select_one('.gdf-web-content')

            if not original_content:
                self._log_validation_error(source_url, "Content Error", 
                    "Conteúdo não encontrado (#conteudo ou .col-md-8.col-md-offset-1)", 
                    destination_url)
                return False

            if not migrated_content:
                self._log_validation_error(source_url, "Content Error", 
                    "Div .gdf-web-content não encontrada", 
                    destination_url)
                return False

            # Verifica se existem colapsáveis - agora procurando em toda a página
            has_collapsibles = False
            
            # Verifica formato original - procura em todo o documento
            if (original_soup.find_all('div', class_='panel-default') or 
                original_soup.find_all('div', class_='panel-heading')):
                has_collapsibles = True
                
            # Verifica formato migrado - procura em todo o documento
            if (migrated_soup.find_all('button', class_='btn-unstyled') or 
                migrated_soup.find_all('div', class_='panel-collapse')):
                has_collapsibles = True

            all_valid = True
            
            if has_collapsibles:
                # Compara colapsáveis usando o documento inteiro
                differences = self._compare_collapsibles(original_soup, migrated_soup)
                if differences:
                    self._log_validation_error(
                        source_url,
                        "Collapsible Mismatch",
                        differences,
                        destination_url
                    )
                    all_valid = False

            # Compara o conteúdo regular da .gdf-web-content
            regular_content_original = self._normalize_text(self.normalize_content(str(original_content)))
            regular_content_migrated = self._normalize_text(self.normalize_content(str(migrated_content)))

            if regular_content_original != regular_content_migrated:
                diff_details = self._get_content_diff(regular_content_original, regular_content_migrated)
                self._log_validation_error(
                    source_url, 
                    "Content Mismatch", 
                    f"Conteúdo principal não corresponde ao original:\n{diff_details}",
                    destination_url
                )
                all_valid = False

            # Verifica imagens e links
            if not self.check_images(str(migrated_content), destination_url):
                all_valid = False

            base_domain = urlparse(source_url).netloc
            if not self.check_relative_links(str(migrated_content), destination_url, base_domain):
                all_valid = False

            # Verifica menus
            menu_diff = self._compare_menus(original_soup, migrated_soup)
            if menu_diff:
                self._log_validation_error(
                    source_url,
                    "Menu Mismatch",
                    f"Menu não corresponde ao original:\n{menu_diff}",
                    destination_url
                )
                all_valid = False

            if all_valid:
                logger.info(f"✓ Página {title} validada com sucesso")
                
            return all_valid

        except Exception as e:
            self._log_validation_error(
                source_url,
                "Validation Error",
                f"Erro durante validação: {str(e)}\n{traceback.format_exc()}",
                destination_url
            )
            return False
    
    def generate_validation_report(self):
        """Gera relatório final da validação"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(self.logs_dir, f"validation_report_{timestamp}.txt")
        
        try:
            with open(report_file, "w", encoding="utf-8") as f:
                f.write("=== Relatório de Validação ===\n\n")
                
                if not self.errors:
                    f.write("Nenhum erro encontrado!\n")
                    return
                    
                for url, errors in self.errors.items():
                    f.write(f"\nURL: {url}\n")
                    for error in errors:
                        f.write(f"- {error}\n")
                    f.write("-" * 50 + "\n")
                    
            logger.info(f"Relatório de validação gerado: {report_file}")
            
        except Exception as e:
            logger.error(f"Erro ao gerar relatório de validação: {str(e)}")