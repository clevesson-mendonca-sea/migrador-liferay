import asyncio
import logging
import traceback
import aiohttp
from dotenv import load_dotenv
import os
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from page_creator import PageCreator
from web_content_folder import FolderCreator
from web_content_creator import WebContentCreator
from document_creator import DocumentCreator
from content_validator import ContentValidator
import argparse
from url_utils import UrlUtils

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Config:
    liferay_url = os.getenv('LIFERAY_URL', '')
    liferay_user = os.getenv('LIFERAY_USERNAME', '')
    liferay_pass = os.getenv('LIFERAY_PASSWORD', '')
    site_id = os.getenv('LIFERAY_SITE_ID', '')
    sheet_id = os.getenv('SPREADSHEET_ID', '')
    folder_type = os.getenv('FOLDER_TYPE', 'journal')
    content_structure_id = os.getenv('LIFERAY_CONTENT_STRUCTURE_ID', '')
    colapse_structure_id = os.getenv('LIFERAY_COLAPSE_STRUCTURE_ID', '')

def parse_hierarchy(hierarchy_str: str) -> list:
    if not hierarchy_str:
        return ['Raiz']
    return [x.strip() for x in hierarchy_str.split('>')]

def filter_hierarchy(hierarchy_str: str) -> list:
    """
    Filtra a hierarquia ignorando os termos específicos.
    Retorna apenas os itens válidos.
    """
    if not hierarchy_str:
        return []
        
    ignored_terms = {'raiz', 'hierarquia'}
    return [
        item.strip() 
        for item in hierarchy_str.split('>')
        if item.strip().lower() not in ignored_terms
    ]

async def get_sheet_data():
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', ['https://www.googleapis.com/auth/spreadsheets.readonly'])
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'client_secret.json',
            ['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    gc = gspread.authorize(creds)
    workbook = gc.open_by_key(Config.sheet_id)
    
    sheet = next(
      sheet for sheet in workbook.worksheets() 
      if "mapeamento" in sheet.title.lower()
    )
    rows = sheet.get_all_values()[1:]

    page_type = [
        row[14] if len(row) > 14 and row[14].strip() not in ["", "-"] else "widget"
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

    # Obtém os tipos de coluna (1 Coluna, 30/70)
    column_type = [
        row[18] if len(row) > 18 and row[18].strip() not in ["", "-"] else "1_column"
        for row in rows
    ]

    # Faz a conversão para os tipos do Liferay
    column_type_formatted = [
        "1_column" if item.strip().lower() == "1 coluna" else
        "2_columns_ii" if item.strip().lower() == "30/70" else
        "1_column"
        for item in column_type
    ]

    url_utils = UrlUtils()
    base_domain = url_utils.extract_domain(Config.liferay_url)

    pages = []
    for index, row in enumerate(rows):
        if all(row[:1]) and len(row) > 6 and row[6]:
            hierarchy = filter_hierarchy(row[6])
            
            if hierarchy:
                title = hierarchy[-1]
                visibility = row[7].strip().lower() if len(row) > 7 and row[7] else 'menu'
                is_visible = visibility == 'menu'
                
                # Get source and destination URLs
                source_url = row[0].strip() if row[0] else ''
                dest_url = row[1].strip() if len(row) > 1 and row[1] else ''
                
                # Build complete URLs using UrlUtils
                complete_source_url = url_utils.build_url(source_url, base_domain)
                complete_dest_url = url_utils.build_url(dest_url, base_domain)
                
                if title.strip():
                    page_data = {
                        'title': title,
                        'url': complete_source_url,
                        'destination': complete_dest_url,
                        'hierarchy': hierarchy,
                        'type': page_type_formatted[index],
                        'visible': is_visible,
                        "column_type": column_type_formatted[index]
                    }
                    pages.append(page_data)

    logger.info(f"Total de páginas processadas: {len(pages)}")
    return pages

async def migrate_pages(pages):
    config = Config()
    creator = PageCreator(config)
    
    auth = aiohttp.BasicAuth(
        login=config.liferay_user,
        password=config.liferay_pass
    )

    async with aiohttp.ClientSession(
        auth=auth,
        headers={"Content-Type": "application/json"},
        connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        creator.session = session

        for page in pages:
            logger.info(f"\nProcessando página: {page['title']}")
            logger.info(f"Hierarquia: {' > '.join(page['hierarchy'])}")
            logger.info(f"Tipo de pagina: {page['type']}")

            page_id = await creator.create_hierarchy(
                hierarchy=page['hierarchy'],
                final_title=page['title'],
                final_url=page['url'].strip('/').split('/')[-1],
                page_type=page['type'],
                visible=page['visible'],
                column_type=page['column_type']
            )
            
            if page_id:
                logger.info(f"Página criada: {page['title']} (ID: {page_id}) tipo({page['type']})")
            else:
                logger.error(f"Falha ao criar página: {page['title']} {page['type']}")

        await creator.retry_failed_pages()
        
async def migrate_folders(pages):
    config = Config()
    folder_creator = FolderCreator(config)
    
    try:
        await folder_creator.initialize_session()
        
        for page in pages:
            logger.info(f"\nProcessando pasta: {page['title']}")
            logger.info(f"Hierarquia: {' > '.join(page['hierarchy'])}")
            
            folder_id = await folder_creator.create_folder_hierarchy(
                hierarchy=page['hierarchy'],
                final_title=page['title'],
                folder_type=config.folder_type
            )
            
            if folder_id:
                logger.info(f"Pasta criada: {page['title']} (ID: {folder_id})")
            else:
                logger.error(f"Falha ao criar pasta: {page['title']}")

        await folder_creator.retry_failed_folders()
    finally:
        await folder_creator.close()

async def migrate_contents(pages):
    config = Config()
    content_creator = WebContentCreator(config)
    content_mapping = {}

    try:
        await content_creator.initialize_session()
        
        for page in pages:
            logger.info(f"\nProcessando conteúdo: {page['title']}")
            logger.info(f"Hierarquia: {' > '.join(page['hierarchy'])}")

            content_id = await content_creator.migrate_content(
                source_url=page['url'],
                title=page['title'],
                hierarchy=page['hierarchy']
            )
            
            if content_id:
                logger.info(f"Conteúdo migrado: {page['title']} (ID: {content_id})")
                content_mapping[page['title']] = content_id
            else:
                logger.error(f"Falha ao migrar conteúdo: {page['title']}")

    finally:
        await content_creator.close()
    
    return content_mapping

async def migrate_documents(pages):
    config = Config()
    doc_creator = DocumentCreator(config)
    folder_creator = FolderCreator(config)
    
    try:
        await doc_creator.initialize_session()
        await folder_creator.initialize_session()
        
        folder_cache = {}
        
        for page in pages:
            if not page['url'] or not isinstance(page['url'], str):
                logger.warning(f"Pulando URL inválida: {page['url']}")
                continue
                
            logger.info(f"\nProcessando página: {page['url']}")
            logger.info(f"Hierarquia: {' > '.join(page['hierarchy'])}")
            
            try:
                # Busca ou cria a pasta usando cache
                hierarchy_key = tuple(page['hierarchy'])
                if hierarchy_key not in folder_cache:
                    folder_id = await folder_creator.create_folder_hierarchy(
                        hierarchy=page['hierarchy'],
                        final_title=page['hierarchy'][-1],
                        folder_type='documents'
                    )
                    folder_cache[hierarchy_key] = folder_id
                    print(page['title'])
                else:
                    folder_id = folder_cache[hierarchy_key]
                
                if folder_id:
                    logger.info(f"Usando pasta com ID: {folder_id}")
                    
                    # Processa a página e seus arquivos
                    migrated_urls = await doc_creator.process_page_content(
                        page_url=page['url'],
                        folder_id=folder_id
                    )
                    
                    if migrated_urls:
                        logger.info(f"✓ Arquivos migrados da página {page['url']}:")
                        for url in migrated_urls:
                            logger.info(f"  - {url}")
                    else:
                        logger.warning(f"Nenhum arquivo encontrado/migrado da página {page['url']}")
                        
                else:
                    logger.error(f"✗ Não foi possível encontrar/criar pasta para a página: {page['url']}")
            
            except Exception as e:
                logger.error(f"Erro ao processar página {page['url']}: {str(e)}")
                logger.error(traceback.format_exc())
                continue
                
    finally:
        await doc_creator.close()
        await folder_creator.close()

async def validate_content(pages):
    config = Config()
    validator = ContentValidator(config)
    
    try:
        await validator.initialize_session()
        
        for page in pages:
            logger.info(f"\nValidando página: {page['title']}")
            logger.info(f"URL Original: {page['url']}")
            logger.info(f"URL Migrada: {page['destination']}")
            
            is_valid = await validator.validate_page(
                source_url=page['url'],
                destination_url=page['destination'],
                title=page['title']
            )
            
            if is_valid:
                logger.info(f"✓ Página validada com sucesso: {page['title']}")
            else:
                logger.error(f"✗ Erros encontrados na página: {page['title']}")

    finally:
        await validator.close()

async def main():
    parser = argparse.ArgumentParser(description='Migração de conteúdo Liferay')
    parser.add_argument('--folders', action='store_true', help='Migrar apenas pastas')
    parser.add_argument('--contents', action='store_true', help='Migrar apenas conteúdos')
    parser.add_argument('--pages', action='store_true', help='Migrar apenas páginas')
    parser.add_argument('--documents', action='store_true', help='Migrar apenas documentos')
    parser.add_argument('--validate', action='store_true', help='Validar conteúdo migrado')
    args = parser.parse_args()

    try:
        url_utils = UrlUtils()
        
        # Get and process sheet data
        pages = await get_sheet_data()

        if not pages:
            logger.error("Nenhuma página válida encontrada na planilha")
            return

        content_mapping = {}

        if args.validate:
            logger.info("Iniciando validação do conteúdo...")
            await validate_content(pages)
            return
        
        if args.documents:
            logger.info("Iniciando migração de documentos...")
            await migrate_documents(pages)
        if args.folders:
            logger.info("Iniciando migração de pastas...")
            await migrate_folders(pages)
        elif args.contents:
            logger.info("Iniciando migração de conteúdos...")
            content_mapping = await migrate_contents(pages)
        elif args.pages:
            logger.info("Iniciando migração de páginas...")
            await migrate_pages(pages)
        else:
            logger.info("Iniciando migração completa...")
            await migrate_pages(pages)
            await migrate_folders(pages)
            content_mapping = await migrate_contents(pages)
            await migrate_pages(pages)

        if content_mapping:
            config = Config()
            content_creator = WebContentCreator(config)
            try:
                await content_creator.initialize_session()
                await content_creator.add_content_to_created_pages(content_mapping)
            finally:
                await content_creator.close()
            
        logger.info("Migração concluída!")
        
    except KeyboardInterrupt:
        logger.info("Migração interrompida")
    except Exception as e:
        logger.error(f"Erro fatal: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    
if __name__ == "__main__":
   asyncio.run(main())