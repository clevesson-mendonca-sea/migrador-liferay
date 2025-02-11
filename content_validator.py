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
        self.validation_log_file = "validation_results.txt"
        self.content_mismatch_file = "content_mismatch_errors.txt"
        self.menu_mismatch_file = "menu_mismatch_errors.txt"
        self.validation_json_file = "validation_results.json"
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

    def _log_validation_error(self, url: str, error_type: str, details: str):
        """Registra erros de validação"""
        if url not in self.errors:
            self.errors[url] = []
        
        self.errors[url].append(f"{error_type}: {details}")
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_entry = (
            f"\n=== Validation Error: {timestamp} ===\n"
            f"URL: {url}\n"
            f"Type: {error_type}\n"
            f"Details: {details}\n"
            f"{'=' * 50}\n"
        )
        
        # Decide qual arquivo usar baseado no tipo de erro
        if "Conteúdo não corresponde ao original" in details:
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
                "url": url,
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
    
    
    def _extract_collapsibles(self, soup: BeautifulSoup, is_original: bool = True) -> List[dict]:
        """Extrai conteúdo dos colapsáveis"""
        collapsibles = []
        
        if is_original:
            # Formato original
            panels = soup.find_all('div', class_='panel-heading')
            for panel in panels:
                title = panel.find('p', class_='panel-title')
                content_div = panel.find_next('div', class_='panel-collapse')
                
                if title and content_div:
                    collapsibles.append({
                        'title': title.get_text(strip=True),
                        'content': self.normalize_content(str(content_div.find('div', class_='panel-body')))
                    })
        else:
            # Formato migrado
            buttons = soup.find_all('button', class_='btn-unstyled')
            for button in buttons:
                title = button.find('span', class_='panel-title')
                content_id = button.get('aria-controls')
                if title and content_id:
                    content_div = soup.find('div', id=content_id)
                    if content_div:
                        collapsibles.append({
                            'title': title.get_text(strip=True),
                            'content': self.normalize_content(str(content_div.find('div', class_='panel-body')))
                        })
        
        return collapsibles

    def _compare_collapsibles(self, original_soup: BeautifulSoup, migrated_soup: BeautifulSoup) -> Optional[str]:
        """Compara os colapsáveis entre as páginas"""
        original_collapsibles = self._extract_collapsibles(original_soup, True)
        migrated_collapsibles = self._extract_collapsibles(migrated_soup, False)
        
        if not original_collapsibles and not migrated_collapsibles:
            return None
            
        if len(original_collapsibles) != len(migrated_collapsibles):
            return f"Diferença no número de seções colapsáveis: Original ({len(original_collapsibles)}) vs Migrado ({len(migrated_collapsibles)})"
            
        differences = []
        for i, (orig, mig) in enumerate(zip(original_collapsibles, migrated_collapsibles)):
            # Compara títulos
            if orig['title'].lower().strip('?') != mig['title'].lower().strip('?'):
                differences.append(f"Seção {i+1}: Título diferente\nOriginal: '{orig['title']}'\nMigrado: '{mig['title']}'")
            
            # Compara conteúdo
            if orig['content'] != mig['content']:
                diff_details = self._compare_content(orig['content'], mig['content'])
                differences.append(f"Seção {i+1}: {diff_details}")
                
        if differences:
            return "Diferenças nos colapsáveis:\n" + "\n".join(differences)
            
        return None

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
        """Extrai links do menu lateral"""
        menu = soup.select_one('.menu-lateral-flutuante' if is_original else '.list-menu')
        if not menu:
            return []
            
        links = []
        for link in menu.find_all('a'):
            text = link.get_text(strip=True)
            href = link.get('href', '')
            if text and href and 'display: none' not in str(link.parent.get('style', '')):
                links.append((text, href))
                
        return links

    def _compare_menus(self, original_soup: BeautifulSoup, migrated_soup: BeautifulSoup) -> Optional[str]:
        """Compara os menus laterais e retorna detalhes das diferenças"""
        original_links = self._extract_menu_links(original_soup, True)
        migrated_links = self._extract_menu_links(migrated_soup, False)
        
        if not original_links and not migrated_links:
            return None
            
        if len(original_links) != len(migrated_links):
            return f"Diferença no número de links: Original ({len(original_links)}) vs Migrado ({len(migrated_links)})"
            
        differences = []
        for i, ((orig_text, orig_href), (mig_text, mig_href)) in enumerate(zip(original_links, migrated_links)):
            if orig_text != mig_text:
                differences.append(f"Link {i+1}: Texto diferente - Original: '{orig_text}' vs Migrado: '{mig_text}'")
                
            orig_path = urlparse(orig_href).path
            mig_path = urlparse(mig_href).path
            if orig_path != mig_path:
                differences.append(f"Link {i+1}: URL diferente - Original: '{orig_path}' vs Migrado: '{mig_path}'")
                
        if differences:
            return "Diferenças no menu:\n" + "\n".join(differences)
            
        return None

    def _compare_content(self, original: str, migrated: str) -> str:
        """Compara conteúdos e retorna detalhes das diferenças"""
        original_words = original.split()
        migrated_words = migrated.split()
        
        # Usa difflib para encontrar diferenças
        differ = difflib.SequenceMatcher(None, original_words, migrated_words)
        
        differences = []
        for tag, i1, i2, j1, j2 in differ.get_opcodes():
            if tag != 'equal':
                orig = ' '.join(original_words[i1:i2])
                mig = ' '.join(migrated_words[j1:j2])
                differences.append(f"- {tag}: Original: '{orig}' -> Migrado: '{mig}'")

        if differences:
            return "Diferenças encontradas:\n" + "\n".join(differences)
        
        return "Conteúdos diferentes mas não foi possível identificar as diferenças específicas"

    async def validate_page(self, source_url: str, destination_url: str, title: str) -> bool:
        """Valida o conteúdo entre a página original e a migrada"""
        try:
            logger.info(f"Validando {title}")
            logger.info(f"Original: {source_url}")
            logger.info(f"Migrada: {destination_url}")

            # Busca conteúdo
            original_html = await self.fetch_content(source_url)
            if not original_html:
                self._log_validation_error(source_url, "Fetch Error", "Não foi possível buscar página original")
                return False

            migrated_html = await self.fetch_content(destination_url)
            if not migrated_html:
                self._log_validation_error(destination_url, "Fetch Error", "Não foi possível buscar página migrada")
                return False

            # Parse HTML
            original_soup = BeautifulSoup(original_html, 'html.parser')
            migrated_soup = BeautifulSoup(migrated_html, 'html.parser')

            # Busca conteúdo principal
            original_content = (
                original_soup.select_one('#conteudo') or 
                original_soup.select_one('.col-md-8.col-md-offset-1')
            )
            migrated_content = migrated_soup.select_one('.gdf-web-content')

            if not original_content:
                self._log_validation_error(source_url, "Content Error", "Conteúdo não encontrado (#conteudo ou .col-md-8.col-md-offset-1)")
                return False

            if not migrated_content:
                self._log_validation_error(destination_url, "Content Error", "Div .gdf-web-content não encontrada")
                return False

            # Compara menus
            menu_diff = self._compare_menus(original_soup, migrated_soup)
            if menu_diff:
                self._log_validation_error(
                    source_url,
                    "Menu Mismatch",
                    f"Menu não corresponde ao original:\n{menu_diff}"
                )
                return False

            # Verifica imagens
            if not self.check_images(str(migrated_content), destination_url):
                return False

            # Verifica links relativos
            base_domain = urlparse(source_url).netloc
            if not self.check_relative_links(str(migrated_content), destination_url, base_domain):
                return False

            # Compara conteúdo
            original_text = self.normalize_content(str(original_content))
            migrated_text = self.normalize_content(str(migrated_content))

            if original_text != migrated_text:
                diff_details = self._compare_content(original_text, migrated_text)
                self._log_validation_error(
                    source_url, 
                    "Content Mismatch", 
                    f"Conteúdo não corresponde ao original:\n{diff_details}"
                )
                return False

            logger.info(f"✓ Página {title} validada com sucesso")
            return True

        except Exception as e:
            self._log_validation_error(
                source_url,
                "Validation Error",
                f"Erro durante validação: {str(e)}\n{traceback.format_exc()}"
            )
            return False

    def generate_validation_report(self):
        """Gera relatório final da validação"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"validation_report_{timestamp}.txt"
        
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