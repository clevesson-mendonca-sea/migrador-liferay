import asyncio
import base64
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
from content_validator import ContentValidator
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
   spreadsheet = gc.open_by_key(Config.sheet_id)
   
   # Busca primeira aba que contenha "mapeamento" no título
   worksheet = None
   for sheet in spreadsheet.worksheets():
       if "mapeamento" in sheet.title.lower():
           print(f"Aba encontrada: {sheet.title}")
           worksheet = sheet
           break
           
   if not worksheet:
       raise Exception("Nenhuma aba que contenha 'mapeamento' encontrada na planilha")
       
   rows = worksheet.get_all_values()[1:]

   pages = []
   for row in rows:
       if all(row[:2]) and len(row) > 6 and row[6]:  # Verifica se tem as duas primeiras colunas
           hierarchy = parse_hierarchy(row[6])
           title = hierarchy[-1] if hierarchy else "Sem Título"
           visibility = row[7].strip().lower() if len(row) > 7 and row[7] else 'menu'
           is_visible = visibility == 'menu'

           if title.strip():
               pages.append({
                   'title': title,
                   'source_url': row[0],  # Coluna 0 (De)
                   'destination_url': row[1],  # Coluna 1 (Para)
                   'hierarchy': hierarchy,
                   'visible': is_visible,
               })

   return pages

async def migrate_pages(pages):
    config = Config()
    creator = PageCreator(config)
    auth = base64.b64encode(f"{config.liferay_user}:{config.liferay_pass}".encode()).decode()

    async with aiohttp.ClientSession(headers={
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/x-www-form-urlencoded",
    }, 
        connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        creator.session = session

        for page in pages:
            logger.info(f"\nProcessando página: {page['title']}")
            logger.info(f"Hierarquia: {' > '.join(page['hierarchy'])}")
            
            page_id = await creator.create_hierarchy(
                hierarchy=page['hierarchy'],
                final_title=page['title'],
                final_url=page['destination'].strip('/').split('/')[-1],
                visible=page['visible']
            )

            if page_id:
                logger.info(f"Página criada: {page['title']} (ID: {page_id})")
            else:
                logger.error(f"Falha ao criar página: {page['title']}")

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

async def validate_content(pages):
    config = Config()
    validator = ContentValidator(config)
    
    try:
        await validator.initialize_session()
        
        for page in pages:
            logger.info(f"\nValidando página: {page['title']}")
            logger.info(f"URL Original: {page['source_url']}")
            logger.info(f"URL Migrada: {page['destination_url']}")
            
            is_valid = await validator.validate_page(
                source_url=page['source_url'],
                destination_url=page['destination_url'],
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
   parser.add_argument('--validate', action='store_true', help='Validar conteúdo migrado')
   args = parser.parse_args()

   try:
       pages = await get_sheet_data()

       if not pages:
           logger.error("Nenhuma página válida encontrada na planilha")
           return

       content_mapping = {}

       if args.validate:
           logger.info("Iniciando validação do conteúdo...")
           await validate_content(pages)
           return
           
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