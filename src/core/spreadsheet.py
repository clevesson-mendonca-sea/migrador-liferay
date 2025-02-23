import os
import logging
import traceback
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from configs.config import Config
from core.url_utils import UrlUtils
from utils.hierarchy_utils import filter_hierarchy

logger = logging.getLogger(__name__)

class SheetColumns:
    """Define o mapeamento de colunas da planilha"""
    
    def __init__(self, sheet):
        """
        Inicializa o mapeamento de colunas baseado na estrutura da planilha
        """
        self.column_indices = {}
        self.current_group = None
        self.column_map = {}
        
        all_rows = sheet.get_all_values()
        if not all_rows:
            return
            
        group_headers = all_rows[0]
        subheaders = all_rows[1]
        
        # Mapear grupos e suas colunas
        current_group_index = None
        for i, header in enumerate(group_headers):
            if header.strip():
                current_group_index = i
                self.column_map[current_group_index] = {
                    'name': header,
                    'start': i,
                    'columns': []
                }
            
            if subheaders[i].strip():
                if current_group_index is not None:
                    self.column_map[current_group_index]['columns'].append({
                        'index': i,
                        'name': subheaders[i].strip()
                    })
                    self.column_indices[subheaders[i].strip()] = i
        
        # Verificar colunas obrigatórias
        required_columns = ['De', 'Hierarquia']
        missing_columns = [col for col in required_columns if col not in self.column_indices]
        if missing_columns:
            logger.error(f"Colunas obrigatórias faltando: {missing_columns}")
            for col in missing_columns:
                for subheader_index, subheader in enumerate(subheaders):
                    if col.lower() in subheader.lower():
                        self.column_indices[col] = subheader_index
    
    def get_column_value(self, row: list, column_name: str, default="") -> str:
        """
        Obtém o valor de uma coluna pelo nome
        """
        try:
            index = self.column_indices.get(column_name, -1)
            if index >= 0 and index < len(row):
                return row[index].strip()
            return default
        except Exception as e:
            logger.error(f"Erro ao obter valor da coluna '{column_name}': {str(e)}")
            return default

async def get_sheet_update_data(workbook):
    """
    Extracts update data from the 'noticias' worksheet.
    
    Args:
        workbook: The Google Sheets workbook
        
    Returns:
        list: List of pages to update
    """
    try:
        sheet = next(
            sheet for sheet in workbook.worksheets() 
            if "semob" in sheet.title.lower()
        )
        rows = sheet.get_all_values()

        pages = []
        
        for row in rows:
            # text = " ".join(row).strip()
            # if not text:
            #     continue
                
            # text = text.replace("Article ID:", "ArticleID:")
            
            # if "Title:" in text and "ArticleID:" in text:
            #     parts = text.split("ArticleID:")
            #     if len(parts) == 2:
            #         print(parts)
            #         title = parts[0].replace("Title:", "").strip()
            #         article_id = parts[1].split()[0].strip()
                            
            #         if title and article_id:
            #             pages.append({
            #                 'title': title,
            #                 'article_id': article_id,
            #                 'destination': article_id
            #             })
            
            pages.append({
                'title': row[0],
                'article_id': row[1],
                'destination': row[1]
            })

        return pages
            
    except Exception as e:
        logger.error(f"Erro ao processar planilha de notícias: {str(e)}")
        logger.error(traceback.format_exc())
        return []

async def get_sheet_data(is_update=False):
    """
    Fetches and processes data from Google Sheets.
    
    Args:
        is_update (bool): Whether to fetch update data
        
    Returns:
        list: List of pages data
    """
    config = Config()
    
    # Authenticate with Google Sheets
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_file_dir, '../..'))
    client_secret_path = os.path.join(project_root, 'client_secret.json')
    token_path = os.path.join(project_root, 'token.json')
    
    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, ['https://www.googleapis.com/auth/spreadsheets.readonly'])
        except Exception as e:
            logger.error(f"Erro ao carregar token.json: {str(e)}")
            raise Exception("Falha ao carregar credenciais. Por favor, exclua token.json e tente novamente.")
    else:
        # Check if client_secret.json exists
        if not os.path.exists(client_secret_path):
            error_msg = (
                f"Arquivo {client_secret_path} não encontrado.\n"
                "Este arquivo é necessário para autenticar com o Google Sheets.\n"
                "Siga estas etapas para gerar o arquivo:\n"
                "1. Acesse https://console.cloud.google.com/apis/credentials\n"
                "2. Crie um novo projeto ou selecione um existente\n"
                "3. Configure a tela de consentimento OAuth\n"
                "4. Crie credenciais OAuth 2.0 para 'Aplicativo de Desktop'\n"
                "5. Faça download do arquivo JSON e salve como 'client_secret.json' na raiz do projeto\n"
            )
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
            
        try:
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secret_path,
                ['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            creds = flow.run_local_server(port=0)
            # Save token for future use
            with open(token_path, 'w') as token:
                token.write(creds.to_json())
            logger.info(f"Novas credenciais salvas em {token_path}")
        except Exception as e:
            logger.error(f"Erro ao autenticar com Google Sheets: {str(e)}")
            raise Exception(f"Falha na autenticação Google Sheets: {str(e)}")

    gc = gspread.authorize(creds)
    workbook = gc.open_by_key(config.sheet_id)
    
    if is_update:
        return await get_sheet_update_data(workbook)
    
    try:
        sheet = next(
            sheet for sheet in workbook.worksheets() 
            if "mapeamento" in sheet.title.lower()
        )
        
        columns = SheetColumns(sheet)
        
        all_rows = sheet.get_all_values()[2:]  # Começar da terceira linha
        
        pages = []
        url_utils = UrlUtils()
        base_domain = url_utils.extract_domain(config.liferay_url)
        
        for i, row in enumerate(all_rows, start=3):
            source_url = columns.get_column_value(row, 'De')
            hierarquia = columns.get_column_value(row, 'Hierarquia')
            
            if source_url and hierarquia:
                try:
                    hierarchy = filter_hierarchy(hierarquia)
                    
                    if hierarchy:
                        title = hierarchy[-1]
                        
                        # Obter outros valores
                        visibility = columns.get_column_value(row, 'Visibilidade', 'menu')
                        tipo_pagina = columns.get_column_value(row, 'Tipo de página', 'widget')
                        layout = columns.get_column_value(row, 'Layout', '1 coluna')
                        
                        # Processar tipos
                        page_type = tipo_pagina.lower().replace("página ", "").strip()
                        page_type = (
                            "portlet" if page_type in ["widget", "", "-"] else
                            "node" if "definida" in page_type else
                            "link_to_layout" if "vincular a uma pagina desse site" in page_type else
                            "url" if "vincular a uma url" in page_type else
                            "portlet"
                        )
                        
                        layout_type = (
                            "1_column" if "1 coluna" in layout.lower() else
                            "2_columns_ii" if "30/70" in layout.lower() else
                            "1_column"
                        )
                        
                        if title.strip():
                            page_data = {
                                'title': title,
                                'url': url_utils.build_url(source_url, base_domain),
                                'destination': url_utils.build_url(columns.get_column_value(row, 'Para'), base_domain),
                                'hierarchy': hierarchy,
                                'type': page_type,
                                'visible': visibility.lower() == 'menu',
                                'column_type': layout_type,
                                'menu_title': columns.get_column_value(row, 'Menu lateral'),
                                'url_vincular': columns.get_column_value(row, 'Link da página para a qual redireciona')
                            }
                            pages.append(page_data)
                            # logger.info(f"Página processada: {title}")
                            
                except Exception as e:
                    logger.error(f"Erro processando página: {str(e)}")
                    logger.error(f"Dados da linha: {row}")
        
        if pages:
            logger.info(f"Total de páginas processadas: {len(pages)}")
        else:
            logger.error("Nenhuma página válida encontrada na planilha")
        
        return pages
        
    except Exception as e:
        logger.error(f"Erro ao processar planilha: {str(e)}")
        return []