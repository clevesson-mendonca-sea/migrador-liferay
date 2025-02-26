import logging
import traceback
import asyncio
from tqdm.asyncio import tqdm
from configs.config import Config
from creators.web_content_creator import WebContentCreator
from creators.vocabulary_creator import VocabularyCreator
from updaters.content_update import ContentUpdater

logger = logging.getLogger(__name__)

async def create_vocabulary_and_categories(pages, secretariat_name):
    """
    Cria vocabulário e categorias com base nos dados da planilha.
    
    Args:
        pages (list): Lista de dicionários com dados das páginas

    Returns:
        dict: Mapeamento de nomes de categorias para IDs
    """
    config = Config()
    vocabulary_creator = VocabularyCreator(config)
    categories_mapping = {}
    error_details = []
    
    try:
        await vocabulary_creator.initialize_session()
        
        # Cria o vocabulário
        vocabulary_id = await vocabulary_creator.create_vocabulary(secretariat_name)
        if not vocabulary_id:
            logger.error("❌ Falha ao criar vocabulário. Abortando criação de categorias.")
            return {}
        
        unique_categories = set()
        for page in pages:
            if 'category' in page and page['category'].strip() and page['category'].strip() != "-":
                # Processa a string de categorias (separadas por vírgula)
                categories = page['category'].split(',')
                for cat in categories:
                    cat = cat.strip()
                    if cat.startswith('- '):
                        cat = cat[2:].strip()  # Remove o prefixo "- "
                    if cat:  # Ignora strings vazias
                        unique_categories.add(cat)
        
        # Ordena as categorias
        unique_categories = sorted(list(unique_categories))
        total_categories = len(unique_categories)
        
        if total_categories == 0:
            logger.info("ℹ️ Nenhuma categoria válida encontrada para criar")
            return {}
        
        logger.info(f"📋 Criando {total_categories} categorias para o vocabulário 'Listagem ({secretariat_name})'")
        
        # Cria as categorias
        with tqdm(total=total_categories, desc="Criando categorias", unit="cat") as pbar:
            for index, category_name in enumerate(unique_categories):
                try:
                    category_id = await vocabulary_creator.create_category(category_name)
                    if category_id:
                        categories_mapping[category_name] = category_id
                    else:
                        error_msg = "Falha ao criar categoria - ID não retornado"
                        logger.error(f"❌ {error_msg}: {category_name}")
                        error_details.append({
                            'index': index + 1,
                            'title': category_name,
                            'error': error_msg,
                            'stack': None,
                        })
                except Exception as e:
                    error_msg = str(e)
                    stack_trace = traceback.format_exc()
                    logger.error(f"🚨 Erro ao criar categoria {category_name}: {error_msg}")
                    error_details.append({
                        'index': index + 1,
                        'title': category_name,
                        'error': error_msg,
                        'stack': stack_trace,
                    })
                
                pbar.update(1)  # Atualiza a barra de progresso
        
        total, success = total_categories, len(categories_mapping)
        failed = total - success
        
        logger.info("\n📊 Resumo da Criação de Categorias:")
        logger.info(f"🟢 Sucesso: {success} / 🔴 Falhas: {failed} / 📦 Total: {total}")
        
        # Exibe o relatório detalhado de erros
        if error_details:
            logger.info("\n⚠️ Detalhes dos Erros na Criação de Categorias:")
            logger.info("=" * 80)
            for i, error in enumerate(error_details):
                logger.error(f"Erro #{i+1} - Categoria #{error['index']}")
                logger.error(f"  Nome: {error['title']}")
                logger.error(f"  Erro: {error['error']}")
                if error['stack']:
                    logger.error(f"  Stack Trace:\n{error['stack']}")
                logger.error("-" * 80)
        
        return categories_mapping
    
    finally:
        await vocabulary_creator.close()
        
async def migrate_contents(pages):
    """
    Migra conteúdos e exibe uma barra de progresso.
    A categorização é feita em paralelo.

    Args:
        pages (list): Lista de dicionários com dados das páginas.

    Returns:
        dict: Mapeamento de títulos para IDs de conteúdos migrados.
    """
    config = Config()
    content_creator = WebContentCreator(config)
    content_updater = ContentUpdater(config)
    content_mapping = {}
    error_details = []

    secretariat_name = config.secretariat_name 
    
    # Primeiro, cria o vocabulário e as categorias
    categories_mapping = await create_vocabulary_and_categories(pages, secretariat_name)
    if not categories_mapping and any(page.get('category', '-') != '-' for page in pages):
        logger.warning("⚠️ Não foi possível criar categorias, mas existem páginas com categorias definidas")

    try:
        await content_creator.initialize_session()

        with tqdm(total=len(pages), desc="Migrando conteúdos", unit="page") as pbar:
            for index, page in enumerate(pages):
                title = page['title']
                hierarchy = " > ".join(page['hierarchy'])
                source_url = page['url']
                category = page.get('category', '-')

                logger.info(f"\n📄 Processando: {title} | Hierarquia: {hierarchy}")
                if category and category != '-':
                    logger.info(f"   Categoria: {category}")

                try:
                    # Migra o conteúdo
                    content_id = await content_creator.migrate_content(
                        source_url=source_url, title=title, hierarchy=page['hierarchy']
                    )
                    
                    if content_id:
                        content_mapping[title] = content_id
                        logger.info(f"✅ Migrado: {title} (ID: {content_id})")
                        
                        # Associa a categoria ao conteúdo migrado (em paralelo)
                        if category and category != '-' and categories_mapping:
                            asyncio.create_task(
                                content_updater.associate_category_to_migrated_content(
                                    content_id=content_id,
                                    title=title,
                                    category=category,
                                    categories_mapping=categories_mapping
                                )
                            )
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