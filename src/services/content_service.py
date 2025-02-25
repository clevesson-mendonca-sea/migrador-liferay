import logging
import traceback
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
    error_details = []

    try:
        await content_creator.initialize_session()

        with tqdm(total=len(pages), desc="Migrando conteúdos", unit="page") as pbar:
            for index, page in enumerate(pages):
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
                        error_msg = f"Falha ao migrar - ID não retornado"
                        logger.error(f"❌ {error_msg}: {title}")
                        error_details.append({
                            'index': index + 1,
                            'title': title,
                            'url': source_url,
                            'error': error_msg,
                            'stack': None,
                        })

                except Exception as e:
                    error_msg = str(e)
                    stack_trace = traceback.format_exc()
                    logger.error(f"🚨 Erro ao migrar {title}: {error_msg}")
                    error_details.append({
                        'index': index + 1,
                        'title': title,
                        'url': source_url,
                        'error': error_msg,
                        'stack': stack_trace,
                    })

                pbar.update(1)  # Atualiza a barra de progresso

    finally:
        await content_creator.close()

    total, success = len(pages), len(content_mapping)
    failed = total - success

    logger.info("\n📊 Resumo da Migração:")
    logger.info(f"🟢 Sucesso: {success} / 🔴 Falhas: {failed} / 📦 Total: {total}")

    # Exibe o relatório detalhado de erros
    if error_details:
        logger.info("\n⚠️ Detalhes dos Erros na Migração:")
        logger.info("=" * 80)
        for i, error in enumerate(error_details):
            logger.error(f"Erro #{i+1} - Linha {error['index']} da planilha")
            logger.error(f"  Título: {error['title']}")
            logger.error(f"  URL: {error['url']}")
            logger.error(f"  Erro: {error['error']}")
            if error['stack']:
                logger.error(f"  Stack Trace:\n{error['stack']}")
            logger.error("-" * 80)

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
    error_details = []

    try:
        await content_updater.initialize_session()

        pages_to_update = [page for page in pages if page.get('article_id', '').strip().isdigit()]
        if not pages_to_update:
            logger.warning("⚠ Nenhum artigo encontrado para atualização")
            return {}

        logger.info(f"🔄 Atualizando {len(pages_to_update)} artigos...")

        results = {}

        with tqdm(total=len(pages_to_update), desc="Atualizando conteúdos", unit="article") as pbar:
            for index, page in enumerate(pages_to_update):
                article_id = page['article_id']
                title = page['title']
                original_index = pages.index(page)

                logger.info(f"\n📄 Atualizando: {title} | ID: {article_id}")

                try:
                    success = await content_updater.update_article_content(article_id=article_id, old_url="")
                    results[article_id] = success

                    if success:
                        logger.info(f"✅ Atualizado: {title}")
                    else:
                        error_msg = "Falha na atualização - retornou falso"
                        logger.error(f"❌ {error_msg}: {title}")
                        error_details.append({
                            'index': original_index + 1,
                            'title': title,
                            'article_id': article_id,
                            'error': error_msg,
                            'stack': None,
                        })

                except Exception as e:
                    error_msg = str(e)
                    stack_trace = traceback.format_exc()
                    logger.error(f"🚨 Erro ao atualizar {title}: {error_msg}")
                    error_details.append({
                        'index': original_index + 1,
                        'title': title,
                        'article_id': article_id,
                        'error': error_msg,
                        'stack': stack_trace,
                    })
                    results[article_id] = False

                pbar.update(1)  # Atualiza a barra de progresso

        total, success = len(results), sum(results.values())
        failed = total - success

        logger.info("\n📊 Resumo da Atualização:")
        logger.info(f"🟢 Sucesso: {success} / 🔴 Falhas: {failed} / 📦 Total: {total}")

        # Exibe o relatório detalhado de erros
        if error_details:
            logger.info("\n⚠️ Detalhes dos Erros na Atualização:")
            logger.info("=" * 80)
            for i, error in enumerate(error_details):
                logger.error(f"Erro #{i+1} - Linha {error['index']} da planilha")
                logger.error(f"  Título: {error['title']}")
                logger.error(f"  ID do Artigo: {error['article_id']}")
                logger.error(f"  Erro: {error['error']}")
                if error['stack']:
                    logger.error(f"  Stack Trace:\n{error['stack']}")
                logger.error("-" * 80)

        return results

    finally:
        await content_updater.close()

def export_error_report(error_details, filename="erros_migracao.txt"):
    """
    Exporta um relatório detalhado de erros para um arquivo.

    Args:
        error_details (list): Lista de dicionários com detalhes dos erros.
        filename (str): Nome do arquivo para salvar o relatório.
    """
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write("RELATÓRIO DETALHADO DE ERROS\n")
            f.write("=" * 80 + "\n\n")
            
            for i, error in enumerate(error_details):
                f.write(f"Erro #{i+1} - Linha {error['index']} da planilha\n")
                f.write(f"  Título: {error['title']}\n")
                if 'url' in error:
                    f.write(f"  URL: {error['url']}\n")
                if 'article_id' in error:
                    f.write(f"  ID do Artigo: {error['article_id']}\n")
                f.write(f"  Erro: {error['error']}\n")
                if error['stack']:
                    f.write(f"  Stack Trace:\n{error['stack']}\n")
                f.write("-" * 80 + "\n\n")
        
        logger.info(f"📝 Relatório de erros exportado para: {filename}")
    except Exception as e:
        logger.error(f"❌ Falha ao exportar relatório de erros: {str(e)}")