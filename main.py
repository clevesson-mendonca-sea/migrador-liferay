import asyncio
import base64
import logging
import aiohttp
from dotenv import load_dotenv
import os
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from page_creator import PageCreator
from web_content_folder import FolderCreator
from web_content_creator import WebContentCreator
import argparse

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

def parse_hierarchy(hierarchy_str: str) -> list:
    if not hierarchy_str:
        return ['Raiz']
    return [x.strip() for x in hierarchy_str.split('>')]

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
    sheet = gc.open_by_key(Config.sheet_id).worksheet("Mapeamento Manual")
    rows = sheet.get_all_values()[1:]

    # Primeiro, obtém os tipos originais
    page_type = [
        row[15] if len(row) > 15 and row[15].strip() not in ["", "-"] else "widget"
        for row in rows
    ]

    # Primeiro formato básico (remover espaços, lowercase, etc)
    page_type_formatted = [
        item.lower().replace("tipo de página: ", "").strip()
        for item in page_type
    ]

    # Depois faz a conversão para os tipos do Liferay
    page_type_formatted = [
        "portlet" if item == "widget" else
        "node" if item == "definida" else
        "link_to_layout" if item == "vincular a uma pagina desse site" else
        "url" if item == "vincular a uma url" else
        "portlet"  # default caso nenhuma condição seja atendida
        for item in page_type_formatted
    ]

    # Debug print
    """ for original, converted in zip(page_type, page_type_formatted):
        print(f"Tipo original: '{original}' -> Tipo convertido: '{converted}'") """

    pages = []
    for index, row in enumerate(rows):
        if all(row[:1]) and len(row) > 6 and row[6]:
            hierarchy = parse_hierarchy(row[6])
            title = hierarchy[-1] if hierarchy else "Sem Título"  # Pega o último item da hierarquia

            if title.strip():
                page_data = {
                    'title': title,
                    'url': row[0],
                    'destination': row[0],
                    'hierarchy': hierarchy,
                    'type': page_type_formatted[index]  # Adiciona o tipo de página formatado
                }
                pages.append(page_data)
                # print(pages)
                # Log detalhado para acompanhar os valores sendo processados
                """ print(f"Página processada: {page_data}") """

    # Log final mostrando todas as páginas geradas
    print(f"Total de páginas processadas: {len(pages)}")
    return pages



async def migrate_pages(pages):
    config = Config()
    creator = PageCreator(config)
    auth = base64.b64encode(f"{config.liferay_user}:{config.liferay_pass}".encode()).decode()

    async with aiohttp.ClientSession(headers={
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/x-www-form-urlencoded"
    }) as session:
        creator.session = session

        for page in pages:
            logger.info(f"\nProcessando página: {page['title']}")
            logger.info(f"Hierarquia: {' > '.join(page['hierarchy'])}")
            logger.info(f"Tipo de pagina: {page['type']}")
            
            
            # Log para debug
            
            page_id = await creator.create_hierarchy(
                hierarchy=page['hierarchy'],
                final_title=page['title'],
                final_url=page['destination'].strip('/').split('/')[-1],
                page_type=page['type']
            )
            
            print(page['destination'].strip('/').split('/')[-1])

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

async def main():
    parser = argparse.ArgumentParser(description='Migração de conteúdo Liferay')
    parser.add_argument('--folders', action='store_true', help='Migrar apenas pastas')
    parser.add_argument('--contents', action='store_true', help='Migrar apenas conteúdos')
    parser.add_argument('--pages', action='store_true', help='Migrar apenas páginas')
    args = parser.parse_args()

    try:
        pages = await get_sheet_data()

        if not pages:
            logger.error("Nenhuma página válida encontrada na planilha")
            return

        # Dicionário para mapear títulos de páginas para IDs de conteúdo
        content_mapping = {}

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
        
        # Adiciona conteúdo às páginas
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
        raise
    
if __name__ == "__main__":
    asyncio.run(main())