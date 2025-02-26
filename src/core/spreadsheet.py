import os
import logging
import traceback
import unicodedata
from typing import Dict, List
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from configs.config import Config
from core.url_utils import UrlUtils
from utils.hierarchy_utils import filter_hierarchy

logger = logging.getLogger(__name__)

def authenticate_google_sheets(client_secret_path: str, token_path: str):
    """
    Autentica com o Google Sheets e retorna as credenciais.
    """
    if os.path.exists(token_path):
        try:
            return Credentials.from_authorized_user_file(token_path, ['https://www.googleapis.com/auth/spreadsheets.readonly'])
        except Exception as e:
            logger.error(f"Erro ao carregar token.json: {str(e)}")
            raise Exception("Falha ao carregar credenciais. Exclua token.json e tente novamente.")
    
    if not os.path.exists(client_secret_path):
        raise FileNotFoundError(f"Arquivo {client_secret_path} não encontrado. Siga as instruções no README para gerá-lo.")
    
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secret_path,
        ['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    creds = flow.run_local_server(port=0)
    with open(token_path, 'w') as token:
        token.write(creds.to_json())
    logger.info(f"Novas credenciais salvas em {token_path}")
    return creds

def get_column_mapping(headers: List[str]) -> Dict[str, int]:
    """
    Mapeia os nomes das colunas para seus índices.
    """
    return {header: index for index, header in enumerate(headers)}

def process_page_type(page_type: str) -> str:
    """
    Processa o tipo de página e retorna o formato correto.
    """
    page_type = page_type.lower().replace("página ", "").strip()
    return (
        "portlet" if page_type == "widget" else
        "node" if page_type == "definida" else
        "link_to_layout" if page_type == "vincular a uma pagina desse site" else
        "url" if page_type == "vincular a uma url" else
        "portlet"
    )

def process_column_type(column_type: str) -> str:
    """
    Processa o tipo de layout e retorna o formato correto.
    """
    return (
        "1_column" if column_type.strip().lower() == "1 coluna" else
        "2_columns_ii" if column_type.strip().lower() == "30/70" else
        "1_column"
    )

def get_column_mapping(headers: List[str]) -> Dict[str, int]:
    """
    Mapeia os nomes das colunas para seus índices, removendo acentos e convertendo para minúsculas.
    """
    column_mapping = {}
    for index, header in enumerate(headers):
        # Remove acentos e converte para minúsculas
        normalized_header = ''.join(
            char for char in unicodedata.normalize('NFD', header)
            if unicodedata.category(char) != 'Mn'
        ).lower()
        column_mapping[normalized_header] = index
    return column_mapping

def build_page_data(row: List[str], column_mapping: Dict[str, int], base_domain: str) -> Dict:
    """
    Constrói os dados de uma pagina a partir de uma linha da planilha.
    """
    url_utils = UrlUtils()

    if 'hierarquia' not in column_mapping:
        logger.error("Coluna 'Hierarquia' não encontrada na planilha.")
        return None

    hierarchy = filter_hierarchy(row[column_mapping['hierarquia']])
    if not hierarchy:
        return None

    title = hierarchy[-1]
    visibility = row[column_mapping.get('visibilidade', '')].strip().lower() if 'visibilidade' in column_mapping and len(row) > column_mapping['visibilidade'] else 'menu'
    is_visible = visibility == 'menu'

    menu_title = row[column_mapping.get('menu lateral', '')].strip() if 'menu lateral' in column_mapping and len(row) > column_mapping['menu lateral'] else None
    category = row[column_mapping.get('categoria', '')].strip() if 'categoria' in column_mapping and len(row) > column_mapping['categoria'] else "-"

    source_url = row[column_mapping.get('de', '')].strip() if 'de' in column_mapping and row[column_mapping['de']] else ''
    dest_url = row[column_mapping.get('para', '')].strip() if 'para' in column_mapping and len(row) > column_mapping['para'] and row[column_mapping['para']] else ''
    link_vincular = row[column_mapping.get('link da pagina para a qual redireciona', '')].strip() if 'link da pagina para a qual redireciona' in column_mapping and len(row) > column_mapping['link da pagina para a qual redireciona'] else ""

    return {
        'title': title,
        'url': url_utils.build_url(source_url, base_domain),
        'destination': url_utils.build_url(dest_url, base_domain),
        'hierarchy': hierarchy,
        'type': process_page_type(row[column_mapping.get('tipo de pagina', '')] if 'tipo de pagina' in column_mapping and len(row) > column_mapping['tipo de pagina'] else "widget"),
        'visible': is_visible,
        'column_type': process_column_type(row[column_mapping.get('layout', '')] if 'layout' in column_mapping and len(row) > column_mapping['layout'] else "1_column"),
        'menu_title': menu_title,
        'url_vincular': link_vincular,
        'category': category
    }

async def get_sheet_data(is_update=False):
    """
    Obtém e processa os dados da planilha do Google Sheets.
    
    Args:
        is_update (bool): Se deve buscar dados de atualização.
    
    Returns:
        List[Dict]: Lista de paginas processadas.
    """
    try:
        config = Config()
        current_file_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_file_dir, '../..'))
        client_secret_path = os.path.join(project_root, 'client_secret.json')
        token_path = os.path.join(project_root, 'token.json')

        creds = authenticate_google_sheets(client_secret_path, token_path)
        gc = gspread.authorize(creds)
        workbook = gc.open_by_key(config.sheet_id)

        if is_update:
            return await get_sheet_update_data(workbook)

        sheet = next(sheet for sheet in workbook.worksheets() if "mapeamento" in sheet.title.lower())
        rows = sheet.get_all_values()
        headers = rows[1]
        rows = rows[1:]

        column_mapping = get_column_mapping(headers)
        print(column_mapping)

        # Verifica se as colunas obrigatórias estão presentes
        required_columns = ['hierarquia', 'de', 'para', 'visibilidade', 'menu lateral', 'categoria', 'layout', 'tipo de pagina', 'link da pagina para a qual redireciona']
        missing_columns = [col for col in required_columns if col not in column_mapping]

        if missing_columns:
            logger.error(f"Colunas obrigatórias ausentes na planilha: {missing_columns}")
            raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")

        url_utils = UrlUtils()
        base_domain = url_utils.extract_domain(config.liferay_url)

        # Processa as linhas da planilha
        pages = []
        for row in rows:
            try:
                if all(row[:1]) and len(row) > column_mapping['hierarquia'] and row[column_mapping['hierarquia']]:
                    page_data = build_page_data(row, column_mapping, base_domain)
                    if page_data:
                        pages.append(page_data)
            except Exception as e:
                logger.error(f"Erro ao processar linha: {row}")
                logger.error(f"Detalhes do erro: {str(e)}")
                logger.error(traceback.format_exc())

        logger.info(f"Total de paginas processadas: {len(pages)}")
        return pages

    except Exception as e:
        logger.error(f"Erro ao obter dados da planilha: {str(e)}")
        logger.error(traceback.format_exc())
        raise

async def get_sheet_update_data(workbook):
    """
    Extrai dados de atualização da planilha 'noticias'.
    
    Args:
        workbook: O workbook do Google Sheets.
    
    Returns:
        List[Dict]: Lista de paginas para atualizar.
    """
    try:
        sheet = next(sheet for sheet in workbook.worksheets() if "semob" in sheet.title.lower())
        rows = sheet.get_all_values()

        pages = []
        for row in rows:
            if len(row) >= 2:
                pages.append({
                    'title': row[0].strip(),
                    'article_id': row[1].strip(),
                    'destination': row[1].strip()
                })

        return pages
    except Exception as e:
        logger.error(f"Erro ao processar planilha de notícias: {str(e)}")
        logger.error(traceback.format_exc())
        return []