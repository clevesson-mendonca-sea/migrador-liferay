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
    # Calculate paths relative to project root (not this file's location)
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_file_dir, '../..'))
    client_secret_path = os.path.join(project_root, 'client_secret.json')
    token_path = os.path.join(project_root, 'token.json')
    
    # Check if token.json exists
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
    
    # Get main mapping sheet
    sheet = next(
        sheet for sheet in workbook.worksheets() 
        if "mapeamento" in sheet.title.lower()
    )
    rows = sheet.get_all_values()[1:]

    # Process page types
    page_type = [
        row[12] if len(row) > 12 and row[12].strip() not in ["", "-"] else "widget"
        for row in rows
    ]

    page_type_formatted = [
        item.lower().replace("página ", "").strip()
        for item in page_type
    ]

    page_type_formatted = [
        "portlet" if item == "widget" else
        "node" if item == "definida" else
        "link_to_layout" if item == "vincular a uma pagina desse site" else
        "url" if item == "vincular a uma url" else
        "portlet"
        for item in page_type_formatted
    ]

    # Process column types
    column_type = [
        row[16] if len(row) > 16 and row[16].strip() not in ["", "-"] else "1_column"
        for row in rows
    ]

    column_type_formatted = [
        "1_column" if item.strip().lower() == "1 coluna" else
        "2_columns_ii" if item.strip().lower() == "30/70" else
        "1_column"
        for item in column_type
    ]
    
    # Build page data
    url_utils = UrlUtils()
    base_domain = url_utils.extract_domain(config.liferay_url)

    pages = []
    for index, row in enumerate(rows):
        if all(row[:1]) and len(row) > 8 and row[6]:
            hierarchy = filter_hierarchy(row[6])
            
            if hierarchy:
                title = hierarchy[-1]
                visibility = row[7].strip().lower() if len(row) > 7 and row[7] else 'menu'
                is_visible = visibility == 'menu'
                
                # Extract menu title
                menu_title = row[8].strip() if len(row) > 8 and row[8] else None

                # Get source and destination URLs
                source_url = row[0].strip() if row[0] else ''
                dest_url = row[1].strip() if len(row) > 1 and row[1] else ''
                
                # Build complete URLs
                complete_source_url = url_utils.build_url(source_url, base_domain)
                complete_dest_url = url_utils.build_url(dest_url, base_domain)
                link_vincular = row[15].strip() if len(row) > 15 and row[15] else ""

                if title.strip():
                    page_data = {
                        'title': title,
                        'url': complete_source_url,
                        'destination': complete_dest_url,
                        'hierarchy': hierarchy,
                        'type': page_type_formatted[index],
                        'visible': is_visible,
                        'column_type': column_type_formatted[index],
                        'menu_title': menu_title,
                        "url_vincular": link_vincular
                    }
                    pages.append(page_data)

    logger.info(f"Total de páginas processadas: {len(pages)}")
    return pages