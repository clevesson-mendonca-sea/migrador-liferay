import logging
from configs.config import Config
from creators.web_content_creator import WebContentCreator
from updaters.content_update import ContentUpdater

logger = logging.getLogger(__name__)

async def migrate_contents(pages):
    """
    Migrates web content based on the provided pages.
    
    Args:
        pages (list): List of page data dictionaries
        
    Returns:
        dict: Dictionary mapping page titles to content IDs
    """
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
