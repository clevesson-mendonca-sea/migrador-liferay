import logging
import traceback
from configs.config import Config
from creators.document_creator import DocumentCreator
from creators.folder_creator import FolderCreator

logger = logging.getLogger(__name__)

async def migrate_documents(pages):
    """
    Migrates documents from source pages to Liferay.
    
    Args:
        pages (list): List of page data dictionaries
        
    Returns:
        dict: Dictionary mapping page URLs to lists of migrated document URLs
    """
    config = Config()
    doc_creator = DocumentCreator(config)
    folder_creator = FolderCreator(config)
    
    try:
        await doc_creator.initialize_session()
        await folder_creator.initialize_session()
        
        # Use a cache to avoid creating the same folders multiple times
        folder_cache = {}
        results = {}
        
        for page in pages:
            if not page['url'] or not isinstance(page['url'], str):
                logger.warning(f"Pulando URL inválida: {page['url']}")
                continue
                
            logger.info(f"\nProcessando página: {page['url']}")
            logger.info(f"Hierarquia: {' > '.join(page['hierarchy'])}")
            
            try:
                # Look up or create folder using cache
                hierarchy_key = tuple(page['hierarchy'])
                if hierarchy_key not in folder_cache:
                    folder_id = await folder_creator.create_folder_hierarchy(
                        hierarchy=page['hierarchy'],
                        final_title=page['hierarchy'][-1],
                        folder_type='documents'
                    )
                    folder_cache[hierarchy_key] = folder_id
                else:
                    folder_id = folder_cache[hierarchy_key]
                
                if folder_id:
                    logger.info(f"Usando pasta com ID: {folder_id}")
                    
                    # Process the page and its files
                    migrated_urls = await doc_creator.process_page_content(
                        page_url=page['url'],
                        folder_id=folder_id
                    )
                    
                    if migrated_urls:
                        logger.info(f"✓ Arquivos migrados da página {page['url']}:")
                        for url in migrated_urls:
                            logger.info(f"  - {url}")
                        results[page['url']] = migrated_urls
                    else:
                        logger.warning(f"Nenhum arquivo encontrado/migrado da página {page['url']}")
                        results[page['url']] = []
                        
                else:
                    logger.error(f"✗ Não foi possível encontrar/criar pasta para a página: {page['url']}")
            
            except Exception as e:
                logger.error(f"Erro ao processar página {page['url']}: {str(e)}")
                logger.error(traceback.format_exc())
                results[page['url']] = []
                continue
        
        return results
                
    finally:
        await doc_creator.close()
        await folder_creator.close()