import logging
from configs.config import Config
from creators.page_creator import PageCreator
import aiohttp

logger = logging.getLogger(__name__)

async def migrate_pages(pages):
    """
    Migrates pages based on the provided page data.
    
    Args:
        pages (list): List of page data dictionaries
        
    Returns:
        dict: Dictionary mapping page titles to page IDs
    """
    config = Config()
    creator = PageCreator(config)
    
    # Create auth and session
    auth = aiohttp.BasicAuth(
        login=config.liferay_user,
        password=config.liferay_pass
    )

    # Using context manager to ensure proper cleanup
    async with aiohttp.ClientSession(
        auth=auth,
        headers={"Content-Type": "application/json"},
        connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        creator.session = session
        page_mapping = {}

        for page in pages:
            logger.info(f"\nProcessando página: {page['title']}")
            logger.info(f"Hierarquia: {' > '.join(page['hierarchy'])}")
            logger.info(f"Tipo de pagina: {page['type']}")
            
            if 'menu_title' in page and page['menu_title']:
                logger.info(f"Título do menu: {page['menu_title']}")

            needs_menu = page['column_type'] == '2_columns_ii'
            menu_title = page.get('menu_title') if needs_menu else None
            
            # Extract the URL-friendly name from the full URL
            final_url = page['url'].strip('/').split('/')[-1] if page['url'] else ''
            
            page_id = await creator.create_hierarchy(
                hierarchy=page['hierarchy'],
                final_title=page['title'],
                final_url=final_url,
                page_type=page['type'],
                visible=page['visible'],
                column_type=page['column_type'],
                menu_title=menu_title
            )
            
            if page_id:
                logger.info(f"Página criada: {page['title']} (ID: {page_id}) tipo({page['type']})")
                page_mapping[page['title']] = page_id
                
                if needs_menu and menu_title:
                    logger.info(f"Menu configurado com título: {menu_title}")
            else:
                logger.error(f"Falha ao criar página: {page['title']} {page['type']}")

        # Retry failed pages if needed
        # await creator.retry_failed_pages()
        
        return page_mapping