import logging
import time
import asyncio
import traceback
from urllib.parse import urlparse
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
    
    start_time = time.time()
    total_pages = len(pages)
    processed = 0
    successful = 0
    failed = 0
    
    hierarchy_cache = {}
    
    error_details = []
    
    auth = aiohttp.BasicAuth(
        login=config.liferay_user,
        password=config.liferay_pass
    )
    
    # Timeout mais longo para evitar desconexões
    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(
        auth=auth,
        headers={"Content-Type": "application/json"},
        connector=aiohttp.TCPConnector(
            ssl=False, 
            limit=12,
            keepalive_timeout=60
        ),
        timeout=timeout
    ) as session:
        creator.session = session
        page_mapping = {}
        
        creator.hierarchy_cache = hierarchy_cache
        
        for i, page in enumerate(pages):
            processed += 1
            
            # Mostrar progresso
            progress_pct = (processed / total_pages) * 100
            elapsed_time = time.time() - start_time
            
            logger.info(f"\nProcessando página: {page['title']} ({processed}/{total_pages}, {progress_pct:.1f}%)")
            
            path = urlparse(page['url']).path if page['url'] else ''
            if path.endswith('/'):
                path = path[:-1]
            
            # Usar um pequeno timeout entre solicitações para evitar bloqueios
            if i > 0 and i % 10 == 0:
                await asyncio.sleep(0.5)
            
            try:
                page_id = await asyncio.wait_for(
                    creator.create_hierarchy(
                        hierarchy=page['hierarchy'],
                        final_title=page['title'],
                        final_url=path,
                        page_type=page['type'],
                        visible=page['visible'],
                        column_type=page['column_type'],
                        menu_title=page.get('menu_title'),
                        url_vinculada=page['url_vincular']
                    ), 
                    timeout=30
                )
                
                if page_id:
                    logger.info(f"Página criada: {page['title']} (ID: {page_id}) tipo({page['type']})")
                    page_mapping[page['title']] = page_id
                    successful += 1
                    
                    # Armazenar no cache a hierarquia completa para reutilização
                    for j in range(1, len(page['hierarchy']) + 1):
                        partial_hierarchy = tuple(page['hierarchy'][:j])
                        if partial_hierarchy not in hierarchy_cache:
                            hierarchy_cache[partial_hierarchy] = True
                else:
                    error_msg = "Falha ao criar página - ID não retornado"
                    logger.error(f"Falha ao criar página: {page['title']} {page['type']}")
                    
                    error_details.append({
                        'index': i + 1,
                        'title': page['title'],
                        'url': page['url'],
                        'type': page['type'],
                        'hierarchy': " > ".join(page['hierarchy']),
                        'error': error_msg,
                        'stack': None
                    })
                    
                    failed += 1
            
            except asyncio.TimeoutError:
                error_msg = "Timeout ao processar página"
                logger.error(f"Timeout ao processar página: {page['title']}")
                
                error_details.append({
                    'index': i + 1,
                    'title': page['title'],
                    'url': page['url'],
                    'type': page['type'],
                    'hierarchy': " > ".join(page['hierarchy']),
                    'error': error_msg,
                    'stack': None
                })
                
                failed += 1
                
            except Exception as e:
                error_msg = str(e)
                stack_trace = traceback.format_exc()
                logger.error(f"Erro ao processar página {page['title']}: {error_msg}")
                
                error_details.append({
                    'index': i + 1,
                    'title': page['title'],
                    'url': page['url'],
                    'type': page['type'],
                    'hierarchy': " > ".join(page['hierarchy']),
                    'error': error_msg,
                    'stack': stack_trace
                })
                
                failed += 1

            # Mostrar um resumo a cada 30 páginas
            if processed % 30 == 0 or processed == total_pages:
                avg_time = elapsed_time / processed
                est_remaining = avg_time * (total_pages - processed)
                
                logger.info(f"\n--- Progresso da Migração ---")
                logger.info(f"Progresso: {progress_pct:.1f}% ({processed}/{total_pages})")
                logger.info(f"Sucesso: {successful}, Falhas: {failed}")
                logger.info(f"Tempo decorrido: {elapsed_time:.1f}s, Média/página: {avg_time:.2f}s")
                logger.info(f"Tempo estimado restante: {est_remaining:.1f}s")
        
        total_time = time.time() - start_time
        logger.info(f"\n=== RESUMO FINAL DA MIGRAÇÃO ===")
        logger.info(f"Total de páginas processadas: {total_pages}")
        logger.info(f"Páginas criadas com sucesso: {successful} ({successful/total_pages*100:.1f}%)")
        logger.info(f"Páginas com falha: {failed} ({failed/total_pages*100:.1f}%)")
        logger.info(f"Tempo total: {total_time:.2f} segundos")
        logger.info(f"Média por página: {total_time/total_pages:.2f} segundos")
        
        if error_details:
            logger.info("\n⚠️ DETALHES DOS ERROS NA MIGRAÇÃO DE PÁGINAS:")
            logger.info("=" * 80)
            for i, error in enumerate(error_details):
                logger.error(f"Erro #{i+1} - Linha {error['index']} da planilha")
                logger.error(f"  Título: {error['title']}")
                logger.error(f"  URL: {error['url']}")
                logger.error(f"  Tipo: {error['type']}")
                logger.error(f"  Hierarquia: {error['hierarchy']}")
                logger.error(f"  Erro: {error['error']}")
                if error['stack']:
                    logger.error(f"  Stack Trace:\n{error['stack']}")
                logger.error("-" * 80)
            
            # Exportar relatório de erros para arquivo
            export_error_report(error_details, "erros_migracao_paginas.txt")
        
        return page_mapping

def export_error_report(error_details, filename="erros_migracao.txt"):
    """
    Exporta um relatório detalhado de erros para um arquivo.

    Args:
        error_details (list): Lista de dicionários com detalhes dos erros.
        filename (str): Nome do arquivo para salvar o relatório.
    """
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write("RELATÓRIO DETALHADO DE ERROS - MIGRAÇÃO DE PÁGINAS\n")
            f.write("=" * 80 + "\n\n")
            
            for i, error in enumerate(error_details):
                f.write(f"Erro #{i+1} - Linha {error['index']} da planilha\n")
                f.write(f"  Título: {error['title']}\n")
                f.write(f"  URL: {error['url']}\n")
                f.write(f"  Tipo: {error['type']}\n")
                f.write(f"  Hierarquia: {error['hierarchy']}\n")
                f.write(f"  Erro: {error['error']}\n")
                if error['stack']:
                    f.write(f"  Stack Trace:\n{error['stack']}\n")
                f.write("-" * 80 + "\n\n")
        
        logger.info(f"📝 Relatório de erros exportado para: {filename}")
    except Exception as e:
        logger.error(f"❌ Falha ao exportar relatório de erros: {str(e)}")
