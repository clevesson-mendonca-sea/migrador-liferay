import logging
from tqdm.asyncio import tqdm
from configs.config import Config
from creators.web_content_creator import WebContentCreator
from updaters.content_update import ContentUpdater

logger = logging.getLogger(__name__)

async def migrate_contents(pages):
    """
    Migra conteúdos e exibe uma barra de progresso.

    Args:
        pages (list): Lista de dicionários com dados das páginas.

    Returns:
        dict: Mapeamento de títulos para IDs de conteúdos migrados.
    """
    config = Config()
    content_creator = WebContentCreator(config)
    content_mapping = {}

    try:
        await content_creator.initialize_session()

        with tqdm(total=len(pages), desc="Migrando conteúdos", unit="page") as pbar:
            for page in pages:
                title = page['title']
                hierarchy = " > ".join(page['hierarchy'])
                source_url = page['url']

                logger.info(f"\n📄 Processando: {title} | Hierarquia: {hierarchy}")

                try:
                    content_id = await content_creator.migrate_content(
                        source_url=source_url, title=title, hierarchy=page['hierarchy']
                    )
                    if content_id:
                        content_mapping[title] = content_id
                        logger.info(f"✅ Migrado: {title} (ID: {content_id})")
                    else:
                        logger.error(f"❌ Falha ao migrar: {title}")

                except Exception as e:
                    logger.error(f"🚨 Erro ao migrar {title}: {str(e)}")

                pbar.update(1)  # Atualiza a barra de progresso

    finally:
        await content_creator.close()

    total, success = len(pages), len(content_mapping)
    failed = total - success

    logger.info("\n📊 Resumo da Migração:")
    logger.info(f"🟢 Sucesso: {success} / 🔴 Falhas: {failed} / 📦 Total: {total}")

    return content_mapping

async def update_contents(pages):
    """
    Atualiza conteúdos e exibe uma barra de progresso.

    Args:
        pages (list): Lista de dicionários com dados das páginas.

    Returns:
        dict: Mapeamento de IDs para status de atualização.
    """
    config = Config()
    content_updater = ContentUpdater(config)

    try:
        await content_updater.initialize_session()

        pages_to_update = [page for page in pages if page.get('article_id', '').strip().isdigit()]
        if not pages_to_update:
            logger.warning("⚠ Nenhum artigo encontrado para atualização")
            return {}

        logger.info(f"🔄 Atualizando {len(pages_to_update)} artigos...")

        results = {}

        with tqdm(total=len(pages_to_update), desc="Atualizando conteúdos", unit="article") as pbar:
            for page in pages_to_update:
                article_id = page['article_id']
                title = page['title']

                logger.info(f"\n📄 Atualizando: {title} | ID: {article_id}")

                try:
                    success = await content_updater.update_article_content(article_id=article_id, old_url="")
                    results[article_id] = success

                    if success:
                        logger.info(f"✅ Atualizado: {title}")
                    else:
                        logger.error(f"❌ Falha: {title}")

                except Exception as e:
                    logger.error(f"🚨 Erro ao atualizar {title}: {str(e)}")
                    results[article_id] = False

                pbar.update(1)  # Atualiza a barra de progresso

        total, success = len(results), sum(results.values())
        failed = total - success

        logger.info("\n📊 Resumo da Atualização:")
        logger.info(f"🟢 Sucesso: {success} / 🔴 Falhas: {failed} / 📦 Total: {total}")

        if failed > 0:
            logger.info("❗ Artigos com falha:")
            for article_id, success in results.items():
                if not success:
                    failed_page = next((p for p in pages_to_update if p['article_id'] == article_id), None)
                    logger.error(f"- {failed_page['title']} (ID: {article_id})" if failed_page else f"- ID: {article_id}")

        return results

    finally:
        await content_updater.close()
