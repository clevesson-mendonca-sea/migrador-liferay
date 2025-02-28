import asyncio
import json
import logging
import traceback
from typing import Any, Dict, Optional, List, Tuple
import aiohttp
from bs4 import BeautifulSoup, FeatureNotFound
from requests_cache import Union
from creators.document_creator import DocumentCreator
from creators.folder_creator import FolderCreator

logger = logging.getLogger(__name__)

class ContentUpdater:
    DOCUMENT_PATHS = ['/documents/', '/documento/', 'wp-content', 'wp-conteudo']
    
    def __init__(self, config):
        self.config = config
        self.session = None
        self.doc_creator = DocumentCreator(config)
        self.folder_creator = FolderCreator(config)
        self.base_domain = None 
        
        # Mapeamento de tags e atributos que podem conter URLs
        self._url_tag_selectors = {
            'a': 'href',
            'img': ['src', 'data-src'],
            'source': 'src',
            'iframe': 'src',
            'link': 'href',
            'script': 'src',
            'video': 'src',
            'audio': 'src',
            'embed': 'src',
            'object': 'data',
            'form': 'action'
        }
        
    async def initialize_session(self):
        if self.session:
            await self.session.close()
            
        auth = aiohttp.BasicAuth(
            login=self.config.liferay_user,
            password=self.config.liferay_pass
        )
        
        self.session = aiohttp.ClientSession(
            auth=auth,
            headers={'Content-Type': 'application/json'},
            connector=aiohttp.TCPConnector(ssl=False)
        )
        
        await self.doc_creator.initialize_session()
        await self.folder_creator.initialize_session()

    async def get_content_by_id(self, article_id: str) -> Optional[Dict]:
        try:
            # Primeira tentativa: usando a API Headless Delivery
            headless_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{article_id}"
            
            async with self.session.get(headless_url) as response:
                if response.status == 200:
                    content = await response.json()
                    if content:
                        return content
                    else:
                        logger.info(f"Nenhum conteúdo retornado via Headless API para articleId: {article_id}")
                else:
                    logger.warning(f"Headless API retornou status {response.status} para articleId: {article_id}")
                    try:
                        error_data = await response.json()
                        logger.warning(f"Detalhes do erro Headless: {error_data}")
                    except:
                        pass

            # Segunda tentativa: usando a API JSONWS
            jsonws_url = f"{self.config.liferay_url}/api/jsonws/journal.journalarticle/get-article"
            
            params = {
                'groupId': self.config.site_id,
                'articleId': article_id,
            }
            
            async with self.session.get(jsonws_url, params=params) as response:
                if response.status == 200:
                    content = await response.json()
                    if content and len(content) > 0:
                        return content
                    else:
                        logger.info(f"Nenhum artigo encontrado via JSONWS API para articleId: {article_id}")
                        return None
                
                logger.error(f"Erro ao buscar artigo {article_id} via JSONWS API: {response.status}")
                try:
                    error_data = await response.json()
                    logger.error(f"Detalhes do erro JSONWS: {error_data}")
                except:
                    pass
                return None
                        
        except Exception as e:
            logger.error(f"Erro ao buscar conteúdo {article_id}: {str(e)}")
            return None

    async def create_article_folder(self, article_title: str) -> Optional[str]:
        try:
            # self, title: str, parent_id: int = 0, folder_type: str = 'journal', hierarchy: List[str] = None
            folder_id = await self.folder_creator.create_folder(
                title=article_title,
                parent_id=self.config.news_folder_id,
                folder_type='documents'
            )
            
            if folder_id:
                logger.info(f"Pasta criada para o artigo: {article_title} (ID: {folder_id})")
                return folder_id
            
            logger.error(f"Falha ao criar pasta para o artigo: {article_title}")
            return None
            
        except Exception as e:
            logger.error(f"Erro ao criar pasta para o artigo {article_title}: {str(e)}")
            return None
    
    async def update_article_content(self, article_id: str, old_url: str, page_identifier: Union[str, int] = None) -> bool:
        """
        Atualiza o conteúdo de um artigo, processa links/imagens, torna URLs relativas e opcionalmente associa a uma página
        
        Args:
            article_id: ID do artigo a ser atualizado
            old_url: URL original para processamento de links
            page_identifier: ID ou título da página para associação (opcional)
            
        Returns:
            bool: True se a atualização foi bem-sucedida
        """
        try:
            # Extrai o domínio base da URL antiga para tornar links relativos
            try:
                from urllib.parse import urlparse
                parsed = urlparse(old_url)
                self.base_domain = f"{parsed.scheme}://{parsed.netloc}"
                logger.info(f"Domínio base extraído para relativização: {self.base_domain}")
            except Exception as e:
                logger.warning(f"Não foi possível extrair o domínio base da URL {old_url}: {str(e)}")
                self.base_domain = None

            article = await self.get_content_by_id(article_id)
            if not article:
                logger.error(f"Artigo {article_id} não encontrado")
                return False

            content = article[0] if isinstance(article, list) else article
            
            # Parse o XML para extrair o conteúdo HTML
            try:
                soup = BeautifulSoup(content.get('content', ''), 'xml')
            except FeatureNotFound:
                # Try to import lxml
                import importlib.util
                if importlib.util.find_spec("lxml") is None:
                    logger.error("lxml library is not installed. Please install it with 'pip install lxml'")
                    # Fallback to html.parser
                    soup = BeautifulSoup(content.get('content', ''), 'html.parser')
                    logger.warning("Using html.parser instead of xml parser")
                else:
                    # lxml is installed but still not working
                    logger.error("XML parser not working despite lxml being installed")
                    raise
            
            # Encontra o elemento com o título do artigo
            article_title = article.get("titleCurrentValue")

            if not article_title:
                title_element = soup.find('dynamic-element', attrs={'field-reference': 'call'})

                if title_element:
                    article_title = title_element.find('dynamic-content').text
            
            logger.info(f"Criando pasta para o artigo: {article_title}")
            
            article_folder_id = await self.create_article_folder(article_title)
            if not article_folder_id:
                logger.error(f"Não foi possível criar pasta para o artigo {article_id}")
                return False
            
            content_element = soup.find('dynamic-element', attrs={'field-reference': 'content'})
            if not content_element:
                logger.error(f"Artigo {article_id} não possui elemento de conteúdo")
                return False
                
            dynamic_content = content_element.find('dynamic-content', attrs={'language-id': 'pt_BR'})
            if not dynamic_content:
                logger.error(f"Artigo {article_id} não possui dynamic-content")
                return False
                
            html_content = dynamic_content.text
            
            # Processa imagens e links, incluindo a relativização de URLs
            updated_html = await self.process_content_images(
                content=html_content,
                folder_id=int(article_folder_id),
                base_url=old_url
            )
            
            was_updated = False
            
            if updated_html != html_content:
                dynamic_content.string = f"<![CDATA[{updated_html}]]>"
                externalReference = article.get('externalReferenceCode')
                
                # URL da API headless
                update_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/structured-contents/by-external-reference-code/{externalReference}"
                
                # Payload para a API headless
                payload = {
                    "contentStructureId": self.config.content_structure_id,
                    "title": article_title,
                    "contentFields": [
                        {
                            "name": "content",
                            "contentFieldValue": {
                                "data": updated_html
                            }
                        }
                    ]
                }
                
                logger.info(f"Atualizando conteúdo do artigo {article_id}")
                
                async with self.session.put(update_url, json=payload) as response:
                    if response.status in (200, 201):
                        logger.info(f"✅ Artigo {article_id} atualizado com sucesso")
                        was_updated = True
                    else:
                        logger.error(f"Erro ao atualizar artigo {article_id}: {response.status}")
                        try:
                            error_data = await response.json()
                            logger.error(f"Detalhes do erro: {error_data}")
                        except:
                            pass
                        return False
            else:
                logger.info(f"Nenhuma alteração necessária no conteúdo do artigo {article_id}")
                was_updated = True  # Consideramos sucesso pois não precisou de alterações
            
            # Se a atualização foi bem-sucedida e foi solicitada associação com uma página
            if was_updated and page_identifier:
                logger.info(f"Iniciando associação do artigo {article_id} com a página {page_identifier}")
                
                # Busca a página
                page_data = await self.find_page_by_title_or_id(page_identifier)
                if not page_data:
                    logger.error(f"Página não encontrada: {page_identifier}")
                    # Retorna True pois a atualização foi bem-sucedida mesmo sem a associação
                    return True
                
                # Associa o conteúdo à página
                article_key = article.get('key') or article_id
                association_success = await self.associate_content_with_page_portlet(
                    content=article_key, 
                    page_data=page_data
                )
                
                if association_success:
                    logger.info(f"✅ Artigo {article_id} associado com sucesso à página {page_identifier}")
                else:
                    logger.warning(f"⚠️ Artigo {article_id} atualizado mas associação falhou para página {page_identifier}")
                    # Retorna True pois a atualização foi bem-sucedida mesmo com falha na associação
                    return True
            
            return True

        except Exception as e:
            logger.error(f"Erro ao atualizar/associar artigo {article_id}: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return False
    

    def _clean_url(self, url: str, base_domain: str) -> str:
        """
        Limpa e converte uma URL para formato relativo
        
        Args:
            url: URL para limpar
            base_domain: Domínio base para normalização
            
        Returns:
            URL relativa ou limpa
        """
        try:
            # Remove parâmetros de rastreamento e fragmentos
            url = url.split('#')[0]
            
            # Ignora URLs vazias, mailto: e tel:
            if not url or url.startswith(('mailto:', 'tel:', 'javascript:', '#')):
                return url
            
            # Verifica se a URL já é relativa
            if not url.startswith(('http://', 'https://')):
                return url
            
            # Tenta extrair domínio da URL
            from urllib.parse import urlparse
            parsed = urlparse(url)
            url_domain = f"{parsed.scheme}://{parsed.netloc}"
            
            # Se o domínio for igual ao base_domain, converte para relativa
            if url_domain == base_domain or parsed.netloc == base_domain.replace('https://', '').replace('http://', ''):
                # Retorna apenas o path e a query string
                rel_url = parsed.path
                if parsed.query:
                    rel_url += f"?{parsed.query}"
                return rel_url
                
            # Para domínios diferentes, retorna a URL original
            return url
            
        except Exception as e:
            logger.error(f"Error cleaning URL {url}: {str(e)}")
            return url

    def _make_links_relative(self, html_content: Union[str, BeautifulSoup], base_domain: Optional[str] = None) -> str:
        """
        Limpa e converte URLs para formato relativo no HTML
        
        Args:
            html_content (Union[str, BeautifulSoup]): Conteúdo HTML para processar
            base_domain (Optional[str], optional): Domínio base para normalização de URLs. Defaults to None.
        
        Returns:
            str: Conteúdo HTML com URLs relativas
        """
        # Se for string, converte para BeautifulSoup
        if isinstance(html_content, str):
            soup = BeautifulSoup(html_content, 'html.parser')
        else:
            soup = html_content

        # Se base_domain não for fornecido, tenta obter do atributo da classe
        if base_domain is None:
            base_domain = getattr(self, 'base_domain', None)

        # Se ainda não tiver base_domain, retorna o conteúdo original
        if not base_domain:
            logger.warning("No base domain provided for link relativization")
            return str(soup)

        for tag_name, attrs in self._url_tag_selectors.items():
            for tag in soup.find_all(tag_name):
                if isinstance(attrs, list):
                    for attr in attrs:
                        if url := tag.get(attr):
                            if not any(doc_path in url for doc_path in self.DOCUMENT_PATHS):
                                cleaned_url = self._clean_url(url, base_domain)
                                tag[attr] = cleaned_url
                elif url := tag.get(attrs):
                    if not any(doc_path in url for doc_path in self.DOCUMENT_PATHS):
                        cleaned_url = self._clean_url(url, base_domain)
                        tag[attrs] = cleaned_url

        # Retorna como string
        return str(soup)

    # Versão atualizada da função process_content_images
    async def process_content_images(self, content: str, folder_id: int, base_url: str) -> str:
        """
        Processa imagens e links em um conteúdo HTML, migrando recursos e tornando URLs relativas
        
        Args:
            content: Conteúdo HTML para processar
            folder_id: ID da pasta de documentos
            base_url: URL base para processamento
            
        Returns:
            Conteúdo HTML processado
        """
        # Extrai o domínio base para tornar links relativos
        try:
            from urllib.parse import urlparse
            parsed = urlparse(base_url)
            self.base_domain = f"{parsed.scheme}://{parsed.netloc}"
        except Exception as e:
            logger.warning(f"Não foi possível extrair o domínio base da URL {base_url}: {str(e)}")
            self.base_domain = None

        # Parse o conteúdo HTML
        soup = BeautifulSoup(content, 'html.parser')

        # Processa imagens
        for img in soup.find_all('img'):
            if img.has_attr('srcset'):
                del img['srcset']
                    
            src = img.get('src')

            if src:
                # Corrige se necessário, substituindo 'sedest' por 'sedes'
                src = src.replace("sedest", "sedes")
                
                # Verifica se a URL é relativa
                if not src.startswith(('http://', 'https://')):
                    if '/documents' not in src:
                        src = self.config.wordpress_url + src
                    else:
                        logger.info(f"URL já contém '/documents', não será alterada: {src}")

                img['src'] = src

                if 'wp-content' in src or 'wp-conteudo' in src:
                    try:
                        # Tenta migrar o documento, caso a URL seja válida
                        new_url = await self.doc_creator.migrate_document(
                            doc_url=src,
                            folder_id=folder_id,
                            page_url=base_url
                        )
                        if new_url:
                            img['src'] = new_url
                            logger.info(f"Imagem atualizada: {src} -> {new_url}")
                    except Exception as e:
                        logger.error(f"Erro ao processar imagem {src}: {str(e)}")
        
        # Processa links
        for a in soup.find_all('a'):
            if a.has_attr('href'):
                    
                href = a.get('href')

                if href:
                    # Verifica se a URL é relativa
                    if not href.startswith(('http://', 'https://')):
                        if '/documents' not in href:
                            href = self.config.wordpress_url + href
                        else:
                            logger.info(f"URL já contém '/documents', não será alterada: {href}")

                    a['href'] = href

                    if 'wp-content' in href or 'wp-conteudo' in href:
                        try:
                            # Tenta migrar o documento, caso a URL seja válida
                            new_url = await self.doc_creator.migrate_document(
                                doc_url=href,
                                folder_id=folder_id,
                                page_url=base_url
                            )
                            if new_url:
                                a['href'] = new_url
                                logger.info(f"Link atualizado: {href} -> {new_url}")
                        except Exception as e:
                            logger.error(f"Erro ao processar link {href}: {str(e)}")

        # Após processar imagens e links, torna os links restantes relativos
        if self.base_domain:
            logger.info(f"Tornando URLs relativas com domínio base: {self.base_domain}")
            return self._make_links_relative(soup, self.base_domain)
        else:
            return str(soup)
        
    async def associate_category_to_migrated_content(self, content_id, title: str, category: str, categories_mapping: Dict[str, int]) -> bool:
        """
        Associa categorias a um conteúdo migrado.

        Args:
            content_id: ID do conteúdo (pode ser um dict, str ou int).
            title: Título do conteúdo para logs.
            category: String de categorias separadas por vírgula.
            categories_mapping: Dicionário de mapeamento de nomes de categorias para IDs.

        Returns:
            bool: True se a associação foi bem-sucedida, False caso contrário.
        """
        try:
            if self.session is None:
                logger.info("Inicializando sessão")
                await self.initialize_session()

            # Extrai o Article ID do content_id
            article_id = self._extract_article_id(content_id)
            if not article_id:
                logger.error(f"Não foi possível extrair um ID numérico válido do content_id: {content_id}")
                return False

            # Processa a string de categorias
            categories = self._process_categories(category)
            if not categories:
                logger.info(f"Nenhuma categoria válida encontrada para '{title}'")
                return False

            # Obtém os IDs das categorias
            category_ids = self._get_category_ids(categories, categories_mapping, title)
            if not category_ids:
                logger.warning(f"Nenhuma categoria válida encontrada no mapeamento para '{title}'")
                return False

            # Busca o artigo pelo ID
            article = await self.get_content_by_id(article_id)
            if not article:
                logger.error(f"Não foi possível obter o artigo com ID {article_id}")
                return False

            # Extrai o ID do artigo
            article_id = self._extract_article_id(article)
            if not article_id:
                logger.error("Artigo não possui ID numérico")
                return False

            update_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/structured-contents/{article_id}"

            payload = {'taxonomyCategoryIds': category_ids}

            async with self.session.patch(update_url, json=payload) as update_response:
                if update_response.status in [200, 201, 204]:
                    response_data = await update_response.json()
                    logger.info(f"✅ Categorias {categories} associadas ao conteúdo '{title}'")
                    return True

                logger.error(f"Erro ao associar categorias {categories} ao conteúdo '{title}': {update_response.status}")
                try:
                    error_data = await update_response.json()
                    logger.error(f"Detalhes do erro: {error_data}")
                except:
                    pass
                return False

        except Exception as e:
            logger.error(f"Erro ao associar categorias ao conteúdo '{title}': {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return False

    def _process_categories(self, category: str) -> List[str]:
        """
        Processa a string de categorias separadas por vírgula.

        Args:
            category: String de categorias.

        Returns:
            List[str]: Lista de categorias processadas.
        """
        if not category or category.strip() == "-":
            return []

        categories = []
        for cat in category.split(','):
            cat = cat.strip()
            if cat.startswith('- '):
                cat = cat[2:].strip()  # Remove o prefixo "- "
            if cat:  # Ignora strings vazias
                categories.append(cat)
        return categories

    def _get_category_ids(self, categories: List[str], categories_mapping: Dict[str, int], title: str) -> List[int]:
        """
        Obtém os IDs das categorias com base no mapeamento.

        Args:
            categories: Lista de nomes de categorias.
            categories_mapping: Dicionário de mapeamento de nomes para IDs.
            title: Título do conteúdo para logs.

        Returns:
            List[int]: Lista de IDs das categorias.
        """
        category_ids = []
        for cat in categories:
            if cat in categories_mapping:
                category_ids.append(categories_mapping[cat])
            else:
                logger.warning(f"Categoria '{cat}' não encontrada no mapeamento para '{title}'")
        return category_ids

    def _extract_article_id(self, content_id):
        """
        Extrai o ID numérico do content_id, que pode ser um dict, str ou int.

        Args:
            content_id: ID do conteúdo em vários formatos.

        Returns:
            str: ID numérico extraído ou None se inválido.
        """
        article_id = None

        # Formato 1: {'id': {'id': 416227, 'key': '416225'}, 'key': "{'id': 416227, 'key': '416225'}"}
        if isinstance(content_id, dict) and 'id' in content_id and isinstance(content_id['id'], dict) and 'id' in content_id['id']:
            article_id = content_id['id']['id']
        
        # Formato 2: {'id': 416308, 'key': '416306'}
        elif isinstance(content_id, dict) and 'id' in content_id:
            article_id = content_id['id']
        
        # Formato 3: content_id é uma string ou número diretamente
        elif isinstance(content_id, (str, int)):
            article_id = str(content_id)
        
        if not article_id:
            logger.error(f"Não foi possível extrair um ID numérico válido do content_id: {content_id}")
            return None

        return article_id
    
    # ASSOCIAÇÃO
    async def _controlled_request(self, method: str, url: str, **kwargs) -> Tuple[int, Any]:
        """Executa requisição HTTP com melhor tratamento de erros"""
        if not self.session:
            await self.initialize_session()
                
        try:
            async with getattr(self.session, method)(url, **kwargs) as response:
                status = response.status
                
                try:
                    if response.content_type == 'application/json':
                        data = await response.json(content_type=None)
                    else:
                        data = await response.text()
                        # Try to parse as JSON even if content-type isn't JSON
                        try:
                            data = json.loads(data)
                        except json.JSONDecodeError:
                            pass
                except Exception as e:
                    logger.warning(f"Error parsing response: {str(e)}")
                    data = await response.text()
                
                return status, data
                    
        except aiohttp.ClientResponseError as e:
            logger.error(f"HTTP error during {method} to {url}: {e.status} - {str(e)}")
            raise
        except aiohttp.ClientError as e:
            logger.error(f"Client error during {method} to {url}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during {method} to {url}: {str(e)}")
            raise

    async def _retry_operation(self, operation, *args, max_retries=3, **kwargs):
        """Wrapper para operações com retry exponencial otimizado"""
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return await operation(*args, **kwargs)
            except aiohttp.ClientResponseError as e:
                # Falhas específicas para erros HTTP
                last_error = e
                if e.status in (429, 503, 504):  # Rate limiting ou serviço indisponível
                    wait_time = 2 * (attempt + 1)
                    logger.warning(f"Rate limit ou serviço indisponível ({e.status}). Tentativa {attempt+1}/{max_retries}. Aguardando {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                elif e.status in (400, 401, 403, 404, 405):  # Erros de cliente - não adianta tentar novamente
                    logger.error(f"Erro de cliente ({e.status}): {str(e)}")
                    raise e
                # Outros erros HTTP - tentar novamente
                if attempt < max_retries - 1:
                    wait_time = 1 * (attempt + 1)
                    logger.warning(f"Erro HTTP ({e.status}). Tentativa {attempt+1}/{max_retries}. Aguardando {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                raise last_error
            except Exception as e:
                # Falhas gerais
                last_error = e
                if attempt < max_retries - 1:
                    wait_time = 1 * (attempt + 1)
                    logger.warning(f"Erro geral: {str(e)}. Tentativa {attempt+1}/{max_retries}. Aguardando {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                raise last_error

    async def find_page_by_title_or_id(self, identifier: Union[str, int]) -> Optional[Dict]:
        """
        Busca uma página pelo título ou ID
        
        Args:
            identifier: Título ou ID da página
            
        Returns:
            Dict contendo dados da página ou None se não encontrada
        """
        try:
            if not self.session:
                await self.initialize_session()

            # Define a URL para busca de páginas
            site_pages_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/site-pages"
            
            # Determina estratégia de busca baseada no tipo de identificador
            search_term = str(identifier).lower()
            is_numeric = search_term.isdigit()
            
            params = {
                'page': 1,
                'pageSize': 100,
                'fields': 'id,title,friendlyUrlPath'
            }
            
            if is_numeric:
                # Se for um ID, adiciona filtro específico
                params['filter'] = f"id eq {search_term}"
            else:
                # Se for título, usa busca de texto
                params['search'] = search_term
            
            status, data = await self._controlled_request('get', site_pages_url, params=params)
            if status != 200:
                logger.error(f"Failed to search pages. Status: {status}")
                return None
                
            items = data.get('items', [])
            if not items:
                return None
                
            # Procura correspondência exata primeiro
            page_data = None
            for item in items:
                item_title = item.get('title', '').lower()
                if (is_numeric and str(item.get('id')) == search_term) or item_title == search_term:
                    page_data = item
                    break
            
            # Se não encontrou exata, tenta parcial
            if not page_data and not is_numeric:
                for item in items:
                    if search_term in item.get('title', '').lower():
                        page_data = item
                        break
            
            if not page_data:
                return None

            # Busca rendered page para obter portlets
            friendly_url = page_data.get('friendlyUrlPath', '').strip('/')
            if not friendly_url:
                # Fallback para id se não tiver friendly URL
                page_id = page_data.get('id')
                if not page_id:
                    return None
                friendly_url = str(page_id)

            rendered_url = f"{self.config.liferay_url}/o/headless-delivery/v1.0/sites/{self.config.site_id}/site-pages/{friendly_url}/rendered-page"
            
            headers = {
                'Accept': 'text/html'
            }
            
            status, rendered_html = await self._controlled_request('get', rendered_url, headers=headers)
            if status == 200:
                # Parse portlets
                portlets = self._parse_content_portlets(rendered_html)
                page_data['portlets'] = portlets
                return page_data
                
            return None
                
        except Exception as e:
            logger.error(f"Error finding page: {str(e)}")
            return None

    def _parse_content_portlets(self, html_content: str) -> List[Dict[str, str]]:
        """Parse portlets de conteúdo de HTML"""
        try:
            from bs4 import BeautifulSoup, Tag
            soup = BeautifulSoup(html_content, 'html.parser')

            portlets = []
            journal_portlets = soup.find_all(
                lambda tag: isinstance(tag, Tag) and tag.get('id', '').startswith('p_p_id_com_liferay_journal_content_web_portlet_JournalContentPortlet')
            )
            
            for portlet in journal_portlets:
                portlet_id = portlet.get('id', '').replace('p_p_id_', '')
                if portlet_id:
                    portlets.append({
                        'portletId': portlet_id,
                        'articleId': ''
                    })
            
            # Se não encontrou nenhum portlet, cria um padrão
            if not portlets:
                DEFAULT_PORTLET_ID = 'com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_JournalCont_'
                portlets.append({
                    'portletId': DEFAULT_PORTLET_ID,
                    'articleId': ''
                })

            return portlets
        except Exception as e:
            logger.error(f"Error parsing portlets: {str(e)}")
            DEFAULT_PORTLET_ID = 'com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_JournalCont_'
            return [{
                'portletId': DEFAULT_PORTLET_ID,
                'articleId': ''
            }]

    async def get_journal_portlet_instance(self, page_data: Dict) -> Optional[str]:
        """
        Obtém o ID do portlet Journal Content disponível na página
        """
        try:
            portlets = page_data.get('portlets', [])
            
            # Primeiro tenta encontrar um portlet sem conteúdo associado
            for portlet in portlets:
                portlet_id = portlet.get('portletId', '')
                if portlet_id and not portlet.get('articleId'):
                    return portlet_id
            
            # Se não encontrou vazio, usa o primeiro disponível
            if portlets:
                portlet_id = portlets[0].get('portletId')
                if portlet_id:
                    return portlet_id
                    
            # Se não encontrou nenhum, usa o padrão
            DEFAULT_PORTLET_ID = 'com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_JournalCont_'
            return DEFAULT_PORTLET_ID
            
        except Exception as e:
            logger.error(f"Error getting portlet instance: {str(e)}")
            DEFAULT_PORTLET_ID = 'com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_JournalCont_'
            return DEFAULT_PORTLET_ID

    async def associate_content_with_page_portlet(self, content: Union[Dict[str, Union[int, str]], str, int], 
                                            page_data: Union[Dict, int, str], 
                                            portlet_index: int = 0) -> bool:
        """
        Associa um conteúdo ao portlet Journal Content de uma página com retry
        
        Args:
            content: Conteúdo a ser associado (dict com 'id' e 'key', str ou int)
            page_data: Dados da página (dict, str ou int)
            portlet_index: Índice do portlet a ser usado (0 = primeiro, 1 = segundo, etc.)
            
        Returns:
            bool: True se a associação foi bem-sucedida
        """
        try:
            # Busca página se necessário
            if not isinstance(page_data, dict):
                page_info = await self.find_page_by_title_or_id(page_data)
                if not page_info:
                    logger.warning(f"Page not found for identifier: {page_data}")
                    return False
            else:
                page_info = page_data
            
            # Obter todos os portlets disponíveis
            portlets = page_info.get('portlets', [])
            
            # Verificar se há portlets suficientes
            if not portlets:
                logger.warning(f"No portlets found for page: {page_info.get('title')}")
                return False
            
            # Selecionar o portlet pelo índice especificado
            if portlet_index >= len(portlets):
                logger.warning(f"Portlet index {portlet_index} out of range, using first available portlet")
                portlet_id = portlets[0].get('portletId', 'com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_JournalCont_')
            else:
                portlet_id = portlets[portlet_index].get('portletId', 'com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_JournalCont_')
            
            # Normaliza ID do portlet
            if not portlet_id.startswith('p_p_id_'):
                portlet_id = portlet_id.replace('p_p_id_', '')

            if portlet_id.endswith('_'):
                portlet_id = portlet_id[:-1]
            
            # Normaliza key do conteúdo
            content_key = content.get('key') if isinstance(content, dict) else str(content)
            
            # URL de associação
            ASSOCIATION_ENDPOINT = '/o/api-association-migrador/v1.0/journal-content/associate-article'
            association_url = f"{self.config.liferay_url}{ASSOCIATION_ENDPOINT}"

            params = {
                'plid': str(page_info.get('id')),
                'portletId': portlet_id,
                'articleId': content_key
            }

            async def associate_content():
                status, result = await self._controlled_request('post', association_url, params=params)
                if status in (200, 201):
                    logger.info(f"[ASSOCIAÇÃO] ✅ {result.get('message')}")
                    return result.get('status') == 'SUCCESS'
                raise Exception(f"[ASSOCIAÇÃO] ❌ {status}, {result}")

            # Tenta associar com retry
            return await self._retry_operation(associate_content, max_retries=4)
            
        except Exception as e:
            logger.error(f"Error associating content to portlet {portlet_index}: {str(e)}")
            return False

    async def update_and_associate_content(self, article_id: str, old_url: str, page_identifier: Union[str, int] = None) -> bool:
        """
        Atualiza o conteúdo de um artigo e opcionalmente o associa a uma página
        
        Args:
            article_id: ID do artigo a ser atualizado
            old_url: URL original para processamento de links
            page_identifier: ID ou título da página para associação (opcional)
            
        Returns:
            bool: True se a operação foi bem-sucedida
        """
        try:
            # Primeiro atualiza o conteúdo
            update_success = await self.update_article_content(article_id, old_url)
            if not update_success:
                logger.error(f"Falha ao atualizar o conteúdo do artigo {article_id}")
                return False
                
            # Se não há página para associar, termina aqui
            if not page_identifier:
                return True
                
            # Busca a página
            page_data = await self.find_page_by_title_or_id(page_identifier)
            if not page_data:
                logger.error(f"Página não encontrada: {page_identifier}")
                return False
                
            # Associa o conteúdo à página
            association_success = await self.associate_content_with_page_portlet(
                article_id, page_data
            )
            
            if not association_success:
                logger.warning(f"Artigo {article_id} atualizado mas associação falhou para página {page_identifier}")
                return False
                
            logger.info(f"✅ Artigo {article_id} atualizado e associado à página {page_identifier} com sucesso")
            return True
                
        except Exception as e:
            logger.error(f"Erro ao atualizar e associar artigo {article_id}: {str(e)}")
            return False
    
    async def close(self):
        if self.session:
            await self.session.close()
        await self.doc_creator.close()
        await self.folder_creator.close()
