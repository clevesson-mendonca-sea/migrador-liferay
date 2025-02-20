import logging
from configs.config import Config
from creators.folder_creator import FolderCreator

logger = logging.getLogger(__name__)

async def migrate_folders(pages):
    """
    Migrates folders based on the hierarchy in the provided pages.
    
    Args:
        pages (list): List of page data dictionaries
        
    Returns:
        dict: Dictionary mapping hierarchy keys to folder IDs
    """
    config = Config()
    folder_creator = FolderCreator(config)
    folder_mapping = {}
    
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
                # Store the mapping using tuple of hierarchy as key
                hierarchy_key = tuple(page['hierarchy'])
                folder_mapping[hierarchy_key] = folder_id
            else:
                logger.error(f"Falha ao criar pasta: {page['title']}")

        # Optional: retry failed folders
        # await folder_creator.retry_failed_folders()
        
        return folder_mapping
        
    finally:
        await folder_creator.close()