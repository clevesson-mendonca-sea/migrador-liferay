import logging
import traceback
import aiohttp
import json
import base64

logger = logging.getLogger(__name__)

class VocabularyCreator:
    """
    Classe responsável pela criação de vocabulários e categorias no Liferay usando APIs Headless.
    """
    
    def __init__(self, config):
        """
        Inicializa o criador de vocabulários.
        
        Args:
            config: Objeto de configuração com credenciais e URLs
        """
        self.config = config
        self.session = None
        self.base_url = f"{config.liferay_url}/o/headless-admin-taxonomy/v1.0"
        self.site_id = config.site_id
        self.vocabulary_id = None
        
    async def initialize_session(self):
        """
        Inicializa a sessão HTTP com autenticação.
        """
        if self.session:
            await self.session.close()
        
        # Preparar cabeçalhos de autenticação
        auth_header = base64.b64encode(
            f"{self.config.liferay_user}:{self.config.liferay_pass}".encode('utf-8')
        ).decode('utf-8')
        
        headers = {
            'Authorization': f'Basic {auth_header}',
            'Content-Type': 'application/json'
        }
        
        self.session = aiohttp.ClientSession(
            headers=headers,
            connector=aiohttp.TCPConnector(ssl=False)
        )
        
        logger.info("Sessão inicializada para o criador de vocabulários (Headless)")
    
    async def close(self):
        """
        Fecha a sessão HTTP.
        """
        if self.session:
            await self.session.close()
            logger.info("Sessão do criador de vocabulários encerrada")
    
    async def create_vocabulary(self, secretariat_name):
        """
        Cria um vocabulário com o nome 'Listagem (Nome da secretaria)'.
        
        Args:
            secretariat_name: Nome da secretaria
            
        Returns:
            int: ID do vocabulário criado ou None em caso de falha
        """
        vocabulary_name = f"Listagem ({secretariat_name})"
        
        try:
            # Endpoint para criar vocabulário
            url = f"{self.base_url}/sites/{self.site_id}/taxonomy-vocabularies"
            
            # Dados para a requisição
            payload = {
                "name": vocabulary_name,
                "description": f"Categorias para a secretaria {secretariat_name}"
            }
            
            # Envia a requisição
            async with self.session.post(url, json=payload) as response:
                if response.status in [200, 201]:
                    result = await response.json()
                    self.vocabulary_id = result.get('id')
                    logger.info(f"✅ Vocabulário '{vocabulary_name}' criado com sucesso (ID: {self.vocabulary_id})")
                    return self.vocabulary_id
                elif response.status == 409:
                    # Vocabulário já existe, buscar o existente
                    logger.info(f"ℹ️ Vocabulário '{vocabulary_name}' já existe. Buscando ID...")
                    return await self._find_vocabulary_by_name(vocabulary_name)
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Falha ao criar vocabulário: {error_text}")
                    return None
        
        except Exception as e:
            logger.error(f"🚨 Erro ao criar vocabulário: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    async def _find_vocabulary_by_name(self, vocabulary_name):
        """
        Busca um vocabulário existente pelo nome.
        
        Args:
            vocabulary_name: Nome do vocabulário a buscar
            
        Returns:
            int: ID do vocabulário encontrado ou None
        """
        try:
            # Endpoint para buscar vocabulários
            url = f"{self.base_url}/sites/{self.site_id}/taxonomy-vocabularies"
            
            # Parâmetros de busca
            params = {
                'search': vocabulary_name,
                'page': 1,
                'pageSize': 1
            }
            
            # Envia a requisição
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    result = await response.json()
                    items = result.get('items', [])
                    
                    if items:
                        vocabulary = items[0]
                        self.vocabulary_id = vocabulary.get('id')
                        logger.info(f"✅ Vocabulário encontrado (ID: {self.vocabulary_id})")
                        return self.vocabulary_id
                    
                    logger.warning(f"⚠️ Nenhum vocabulário encontrado com o nome '{vocabulary_name}'")
                    return None
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Falha ao buscar vocabulário: {error_text}")
                    return None
        
        except Exception as e:
            logger.error(f"🚨 Erro ao buscar vocabulário: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    async def create_category(self, category_name, parent_category_id=None):
        """
        Cria uma categoria no vocabulário.
        
        Args:
            category_name: Nome da categoria a ser criada
            parent_category_id: ID da categoria pai (opcional)
            
        Returns:
            int: ID da categoria criada ou None em caso de falha
        """
        if not self.vocabulary_id:
            logger.error("❌ Não é possível criar categorias sem um vocabulário")
            return None
            
        if not category_name or category_name.strip() == "-":
            logger.info("ℹ️ Nome de categoria inválido ou vazio. Ignorando.")
            return None
        
        try:
            # Endpoint para criar categoria
            url = f"{self.base_url}/taxonomy-vocabularies/{self.vocabulary_id}/taxonomy-categories"
            
            # Preparar payload
            payload = {
                "name": category_name,
                "description": ""
            }
            
            # Adicionar categoria pai, se especificado
            if parent_category_id:
                payload['parentTaxonomyCategoryId'] = parent_category_id
            
            # Envia a requisição
            async with self.session.post(url, json=payload) as response:
                if response.status in [200, 201]:
                    result = await response.json()
                    category_id = result.get('id')
                    logger.info(f"✅ Categoria '{category_name}' criada com sucesso (ID: {category_id})")
                    return category_id
                elif response.status == 409:
                    # Categoria já existe, buscar a existente
                    logger.info(f"ℹ️ Categoria '{category_name}' já existe. Buscando ID...")
                    return await self._find_category_by_name(category_name)
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Falha ao criar categoria '{category_name}': {error_text}")
                    return None
        
        except Exception as e:
            logger.error(f"🚨 Erro ao criar categoria '{category_name}': {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    async def _find_category_by_name(self, category_name):
        """
        Busca uma categoria existente pelo nome.
        
        Args:
            category_name: Nome da categoria a buscar
            
        Returns:
            int: ID da categoria encontrada ou None
        """
        try:
            # Endpoint para buscar categorias
            url = f"{self.base_url}/taxonomy-vocabularies/{self.vocabulary_id}/taxonomy-categories"
            
            # Parâmetros de busca
            params = {
                'search': category_name,
                'page': 1,
                'pageSize': 1
            }
            
            # Envia a requisição
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    result = await response.json()
                    items = result.get('items', [])
                    
                    if items:
                        category = items[0]
                        category_id = category.get('id')
                        logger.info(f"✅ Categoria encontrada (ID: {category_id})")
                        return category_id
                    
                    logger.warning(f"⚠️ Nenhuma categoria encontrada com o nome '{category_name}'")
                    return None
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Falha ao buscar categoria: {error_text}")
                    return None
        
        except Exception as e:
            logger.error(f"🚨 Erro ao buscar categoria: {str(e)}")
            logger.error(traceback.format_exc())
            return None
        