import asyncio
import logging
from configs.config import Config
from creators.web_content_creator import WebContentCreator
from updaters.content_update import ContentUpdater

logger = logging.getLogger(__name__)

BATCH_SIZE = 25
MAX_CONCURRENT_TASKS = 50

async def migrate_contents(pages):
    """
    Migrates web content based on the provided pages using batch processing
    with progress tracking
    """
    config = Config()
    content_creator = WebContentCreator(config)
    content_mapping = {}
    total_pages = len(pages)
    processed_count = 0

    try:
        await content_creator.initialize_session()
        
        batches = [pages[i:i + BATCH_SIZE] for i in range(0, len(pages), BATCH_SIZE)]
        
        for batch in batches:
            batch_results = await _process_batch(batch, content_creator, content_mapping)
            processed_count += len(batch_results)
            completion_percentage = (processed_count / total_pages) * 100
            remaining_percentage = 100 - completion_percentage
            
            logger.info(f"\nProgress Update:")
            logger.info(f"Processed: {processed_count}/{total_pages} items")
            logger.info(f"Completed: {completion_percentage:.1f}%")
            logger.info(f"Remaining: {remaining_percentage:.1f}%")
            
            # Log success/failure counts for this batch
            success_count = sum(1 for result in batch_results if result)
            failure_count = len(batch_results) - success_count
            if failure_count > 0:
                logger.warning(f"Batch failures: {failure_count} items failed in this batch")
                
    finally:
        await content_creator.close()
    
    # Final summary
    total_success = sum(1 for v in content_mapping.values() if v)
    total_failure = total_pages - total_success
    logger.info("\nFinal Migration Summary:")
    logger.info(f"Total items processed: {total_pages}")
    logger.info(f"Successfully migrated: {total_success}")
    logger.info(f"Failed migrations: {total_failure}")
    logger.info(f"Overall success rate: {(total_success/total_pages)*100:.1f}%")
    
    return content_mapping

async def _process_batch(batch, content_creator, content_mapping):
    """
    Processes a batch of pages in parallel and returns results
    """
    tasks = []
    results = []
    
    for page in batch:
        task = asyncio.create_task(_process_single_page(page, content_creator, content_mapping))
        tasks.append(task)
    
    completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
    
    for result in completed_tasks:
        if isinstance(result, Exception):
            logger.error(f"Batch processing error: {str(result)}")
            results.append(False)
        else:
            results.append(result)
    
    return results

async def _process_single_page(page, content_creator, content_mapping):
    """
    Processes a single page with improved error handling and result tracking
    """
    try:
        logger.info(f"\nProcessing content: {page['title']}")
        logger.info(f"Hierarchy: {' > '.join(page['hierarchy'])}")

        content_id = await content_creator.migrate_content(
            source_url=page['url'],
            title=page['title'],
            hierarchy=page['hierarchy']
        )
        
        if content_id:
            logger.info(f"Content migrated: {page['title']} (ID: {content_id})")
            content_mapping[page['title']] = content_id
            return True
        else:
            logger.error(f"Failed to migrate content: {page['title']}")
            return False
            
    except Exception as e:
        logger.error(f"Error processing page {page['title']}: {str(e)}")
        return False

async def update_contents(pages):
    """
    Updates existing content based on article IDs in the provided pages.
    
    Args:
        pages (list): List of page data dictionaries with article_id keys
        
    Returns:
        dict: Dictionary mapping article IDs to update status (boolean)
    """
    config = Config()
    content_updater = ContentUpdater(config)
    
    try:
        await content_updater.initialize_session()

        pages_to_update = [
            page for page in pages
            if page.get('article_id', '').strip().isdigit()
        ]

        if not pages_to_update:
            logger.warning("Nenhum artigo encontrado para atualização")
            return
            
        logger.info(f"Encontrados {len(pages_to_update)} artigos para atualizar")
        
        results = {}
        for page in pages_to_update:
            article_id = page['article_id']
            title = page['title']
            
            logger.info(f"\nProcessando artigo: {title}")
            logger.info(f"ID: {article_id}")
            
            try:
                success = await content_updater.update_article_content(
                    article_id=article_id,
                    old_url=""  # URL não é necessária para atualização
                )
                results[article_id] = success
                
                if success:
                    logger.info(f"✓ Artigo atualizado com sucesso: {title}")
                else:
                    logger.error(f"✗ Falha ao atualizar artigo: {title}")
                    
            except Exception as e:
                logger.error(f"Erro processando artigo {article_id}: {str(e)}")
                results[article_id] = False
                continue

        # Log dos resultados
        success = sum(1 for v in results.values() if v)
        failed = sum(1 for v in results.values() if not v)
        
        logger.info(f"\nResultados da atualização:")
        logger.info(f"Total processado: {len(results)}")
        logger.info(f"Sucesso: {success}")
        logger.info(f"Falhas: {failed}")
        
        if failed > 0:
            logger.info("\nArtigos com falha:")
            for article_id, success in results.items():
                if not success:
                    failed_page = next((p for p in pages_to_update if p['article_id'] == article_id), None)
                    if failed_page:
                        logger.error(f"- {failed_page['title']} (ID: {article_id})")
                    else:
                        logger.error(f"- Article ID: {article_id}")
                    
    finally:
        await content_updater.close()
