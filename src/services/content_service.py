import logging
import asyncio
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set, Union, Tuple
import time
from configs.config import Config
from creators.web_content_creator import WebContentCreator
from updaters.content_update import ContentUpdater

logger = logging.getLogger(__name__)

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

class ResourceMonitor:
    """Monitora recursos do sistema durante a migração"""
    
    def __init__(self, interval: int = 30):
        self.interval = interval
        self.running = False
        self.task = None
        self.stats = []
    
    async def start(self):
        """Inicia o monitoramento"""
        if not HAS_PSUTIL:
            logger.warning("Módulo psutil não encontrado, monitoramento desativado")
            return
            
        self.running = True
        self.task = asyncio.create_task(self._monitor_loop())
        logger.info("Monitoramento de recursos iniciado")
    
    async def stop(self):
        """Para o monitoramento"""
        if not self.running or not self.task:
            return
            
        self.running = False
        try:
            self.task.cancel()
            await asyncio.sleep(0.1)
        except:
            pass
    
    async def _monitor_loop(self):
        """Loop principal de monitoramento"""
        while self.running:
            try:
                if HAS_PSUTIL:
                    cpu_percent = psutil.cpu_percent(interval=0.5)
                    memory = psutil.virtual_memory()
                    logger.info(f"Recursos: CPU {cpu_percent:.1f}% | "
                                f"RAM {memory.percent:.1f}% ({memory.used / (1024**3):.2f} GB)")
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erro no monitoramento: {str(e)}")

async def migrate_contents(pages):
    """
    Migrates web content based on the provided pages.
    
    Args:
        pages (list): List of page data dictionaries
        
    Returns:
        dict: Dictionary mapping page titles to content IDs
    """
    # Detectar recursos do sistema para paralelismo otimizado
    max_concurrent, chunk_size = get_optimal_concurrency()
    
    config = Config()
    content_mapping = {}
    semaphore = asyncio.Semaphore(max_concurrent)
    monitor = ResourceMonitor(interval=60)  # Monitorar a cada 60 segundos
    
    # Iniciar monitoramento de recursos
    await monitor.start()
    
    # Criar pool de creators para reduzir overhead de criação
    creator_pool = []
    for _ in range(min(max_concurrent, 5)):  # Limitar número de instâncias
        creator = WebContentCreator(config)
        await creator.initialize_session()
        creator_pool.append(creator)
    
    logger.info(f"Iniciando migração paralela de {len(pages)} páginas com {max_concurrent} workers")
    
    # Processar página individual com um creator específico
    async def process_page(page, creator):
        """Processa uma página individual com controle de concorrência"""
        async with semaphore:
            logger.info(f"\nProcessando conteúdo: {page['title']}")
            logger.info(f"Hierarquia: {' > '.join(page['hierarchy'])}")
            
            try:
                content_id = await creator.migrate_content(
                    source_url=page['url'],
                    title=page['title'],
                    hierarchy=page['hierarchy']
                )
                
                if content_id:
                    logger.info(f"Conteúdo migrado: {page['title']} (ID: {content_id})")
                    return page['title'], content_id
                else:
                    logger.error(f"Falha ao migrar conteúdo: {page['title']}")
                    return page['title'], None
            except Exception as e:
                logger.error(f"Erro ao processar {page['title']}: {str(e)}")
                return page['title'], None
    
    try:
        # Distribuir páginas para os creators do pool
        tasks = []
        for i, page in enumerate(pages):
            # Selecionar creator do pool de forma rotativa
            creator = creator_pool[i % len(creator_pool)]
            # Criar tarefa para cada página
            task = asyncio.create_task(process_page(page, creator))
            tasks.append(task)
        
        # Executar todas as tarefas em paralelo e aguardar resultados
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed_time = time.time() - start_time
        
        # Processar resultados
        success_count = 0
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Erro durante migração: {str(result)}")
                continue
                
            if result and len(result) == 2:
                title, content_id = result
                if content_id:
                    content_mapping[title] = content_id
                    success_count += 1
        
        # Reportar estatísticas de performance
        pages_per_second = len(pages) / elapsed_time if elapsed_time > 0 else 0
        logger.info(f"Migração concluída em {elapsed_time:.2f} segundos")
        logger.info(f"Taxa de sucesso: {success_count}/{len(pages)} ({success_count/len(pages)*100:.1f}%)")
        logger.info(f"Performance: {pages_per_second:.2f} páginas/segundo")
    
    finally:
        # Parar monitoramento
        await monitor.stop()
        
        # Fechar todos os creators do pool
        close_tasks = [creator.close() for creator in creator_pool]
        await asyncio.gather(*close_tasks)
    
    return content_mapping

async def update_contents(pages):
    """
    Updates existing content based on article IDs in the provided pages.
    
    Args:
        pages (list): List of page data dictionaries with article_id keys
        
    Returns:
        dict: Dictionary mapping article IDs to update status (boolean)
    """
    # Detectar recursos do sistema para paralelismo otimizado
    max_concurrent, chunk_size = get_optimal_concurrency()
    
    config = Config()
    results = {}
    semaphore = asyncio.Semaphore(max_concurrent)
    monitor = ResourceMonitor(interval=60)
    
    # Filtrar páginas com article_id válido
    pages_to_update = [
        page for page in pages
        if page.get('article_id', '').strip().isdigit()
    ]
    
    if not pages_to_update:
        logger.warning("Nenhum artigo encontrado para atualização")
        return {}
    
    logger.info(f"Encontrados {len(pages_to_update)} artigos para atualizar")
    
    # Iniciar monitoramento
    await monitor.start()
    
    # Criar pool de updaters
    updater_pool = []
    for _ in range(min(max_concurrent, 5)):  # Limitar número de instâncias
        updater = ContentUpdater(config)
        await updater.initialize_session()
        updater_pool.append(updater)
    
    async def process_article(page, updater):
        """Processa um artigo individual com controle de concorrência"""
        async with semaphore:
            article_id = page['article_id']
            title = page['title']
            
            logger.info(f"\nProcessando artigo: {title}")
            logger.info(f"ID: {article_id}")
            
            try:
                success = await updater.update_article_content(
                    article_id=article_id,
                    old_url=""  # URL não é necessária para atualização
                )
                
                if success:
                    logger.info(f"✓ Artigo atualizado com sucesso: {title}")
                else:
                    logger.error(f"✗ Falha ao atualizar artigo: {title}")
                
                return article_id, success
                
            except Exception as e:
                logger.error(f"Erro processando artigo {article_id}: {str(e)}")
                return article_id, False
    
    try:
        # Distribuir artigos para os updaters do pool
        tasks = []
        for i, page in enumerate(pages_to_update):
            updater = updater_pool[i % len(updater_pool)]
            task = asyncio.create_task(process_article(page, updater))
            tasks.append(task)
        
        # Processar tudo em paralelo
        start_time = time.time()
        update_results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed_time = time.time() - start_time
        
        # Consolidar resultados
        for result in update_results:
            if isinstance(result, Exception):
                logger.error(f"Erro durante atualização: {str(result)}")
                continue
                
            if result and len(result) == 2:
                article_id, success = result
                results[article_id] = success
    
    finally:
        # Parar monitoramento
        await monitor.stop()
        
        # Fechar todos os updaters
        close_tasks = [updater.close() for updater in updater_pool]
        await asyncio.gather(*close_tasks)
    
    # Log dos resultados
    success = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)
    
    logger.info(f"\nResultados da atualização:")
    logger.info(f"Total processado: {len(results)}")
    logger.info(f"Sucesso: {success}")
    logger.info(f"Falhas: {failed}")
    logger.info(f"Tempo total: {elapsed_time:.2f} segundos")
    
    if failed > 0:
        logger.info("\nArtigos com falha:")
        for article_id, success in results.items():
            if not success:
                failed_page = next((p for p in pages_to_update if p['article_id'] == article_id), None)
                if failed_page:
                    logger.error(f"- {failed_page['title']} (ID: {article_id})")
                else:
                    logger.error(f"- Article ID: {article_id}")
    
    return results

def get_optimal_concurrency():
    """
    Determina os valores ótimos de concorrência com base no ambiente de execução
    
    Returns:
        tuple: (max_concurrent, chunk_size)
    """
    try:
        import multiprocessing
        
        # Obter número de CPUs
        cpu_count = multiprocessing.cpu_count()
        
        if HAS_PSUTIL:
            # Ajustar com base na memória disponível
            mem = psutil.virtual_memory()
            mem_gb = mem.available / (1024**3)  # Memória disponível em GB
            
            # Heurística: 2-4 tasks por CPU dependendo da memória disponível
            cpu_factor = min(4, max(2, int(mem_gb / 2)))
            max_concurrent = min(40, cpu_count * cpu_factor)
        else:
            # Conservador: 3 tasks por CPU
            max_concurrent = min(30, cpu_count * 3)
        
        # Tamanho do chunk proporcional à concorrência
        chunk_size = min(20, max(5, max_concurrent // 4))
        
        logger.info(f"Configuração automática: {max_concurrent} tarefas concorrentes, chunks de {chunk_size}")
        return max_concurrent, chunk_size
    except:
        # Valores padrão conservadores
        logger.info("Usando configuração padrão: 20 tarefas concorrentes, chunks de 10")
        return 20, 10