import logging
from configs.config import Config
from validators.content_validator import ContentValidator

logger = logging.getLogger(__name__)

async def validate_content(pages):
    """
    Validates migrated content by comparing source and destination pages.
    
    Args:
        pages (list): List of page data dictionaries with source and destination URLs
        
    Returns:
        dict: Dictionary mapping page titles to validation status (boolean)
    """
    config = Config()
    validator = ContentValidator(config)
    validation_results = {}
    
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
            
            validation_results[page['title']] = is_valid
            
            if is_valid:
                logger.info(f"✓ Página validada com sucesso: {page['title']}")
            else:
                logger.error(f"✗ Erros encontrados na página: {page['title']}")

        # Log summary
        total = len(validation_results)
        success = sum(1 for v in validation_results.values() if v)
        failed = total - success
        
        logger.info(f"\nResultados da validação:")
        logger.info(f"Total páginas validadas: {total}")
        logger.info(f"Páginas válidas: {success}")
        logger.info(f"Páginas com erros: {failed}")
        
        if failed > 0:
            logger.info("\nPáginas com erros de validação:")
            for title, is_valid in validation_results.items():
                if not is_valid:
                    logger.error(f"- {title}")
                    
        return validation_results

    finally:
        await validator.close()