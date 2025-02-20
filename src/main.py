import asyncio
import logging
import traceback
import argparse
from dotenv import load_dotenv

# Core imports
from core.url_utils import UrlUtils
from configs.config import Config
from core.spreadsheet import get_sheet_data

# Service imports
from services.page_service import migrate_pages
from services.folder_service import migrate_folders
from services.content_service import migrate_contents, update_contents
from services.document_service import migrate_documents
from services.validation_service import validate_content

# Initialize environment
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    parser = argparse.ArgumentParser(description='Migração de conteúdo Liferay')
    parser.add_argument('--folders', action='store_true', help='Migrar apenas pastas')
    parser.add_argument('--contents', action='store_true', help='Migrar apenas conteúdos')
    parser.add_argument('--pages', action='store_true', help='Migrar apenas páginas')
    parser.add_argument('--documents', action='store_true', help='Migrar apenas documentos')
    parser.add_argument('--validate', action='store_true', help='Validar conteúdo migrado')
    parser.add_argument('--update', action='store_true', help='Atualizar conteúdos existentes')
    args = parser.parse_args()

    try:
        # Get and process sheet data
        pages = await get_sheet_data(is_update=args.update)

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
        elif args.folders:
            logger.info("Iniciando migração de pastas...")
            await migrate_folders(pages)
        elif args.contents:
            logger.info("Iniciando migração de conteúdos...")
            content_mapping = await migrate_contents(pages)
        elif args.pages:
            logger.info("Iniciando migração de páginas...")
            await migrate_pages(pages)
        elif args.update:
            logger.info("Iniciando correção de conteudos com falhas...")
            await update_contents(pages)
        else:
            logger.info("Iniciando migração completa...")
            await migrate_pages(pages)
            await migrate_folders(pages)
            content_mapping = await migrate_contents(pages)
            await migrate_pages(pages)  # Is this repeated intentionally?

        if content_mapping:
            logger.info("Adicionando conteúdo às páginas criadas...")
            from creators.web_content_creator import WebContentCreator
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