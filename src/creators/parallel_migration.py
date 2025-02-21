import logging
import asyncio
import time
import os
from functools import lru_cache
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set, Union, Tuple
import concurrent.futures
from datetime import datetime
from configs.config import Config
from creators.web_content_creator import WebContentCreator
from updaters.content_update import ContentUpdater

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

logger = logging.getLogger(__name__)

@dataclass
class MigrationStats:
    """Estatísticas do processo de migração"""
    start_time: float = field(default_factory=time.time)
    end_time: float = 0
    total_pages: int = 0
    success_count: int = 0
    failure_count: int = 0
    skipped_count: int = 0
    content_created: int = 0
    images_processed: int = 0
    documents_processed: int = 0
    
    def calculate_duration(self) -> float:
        """Calcula a duração total em segundos"""
        if not self.end_time:
            self.end_time = time.time()
        return self.end_time - self.start_time
    
    def log_summary(self) -> None:
        """Registra um resumo das estatísticas"""
        duration = self.calculate_duration()
        pages_per_second = self.total_pages / duration if duration > 0 else 0
        
        logger.info("="*50)
        logger.info("RESUMO DA MIGRAÇÃO")
        logger.info("="*50)
        logger.info(f"Total de páginas: {self.total_pages}")
        logger.info(f"Conteúdos criados com sucesso: {self.content_created}")
        logger.info(f"Imagens processadas: {self.images_processed}")
        logger.info(f"Documentos processados: {self.documents_processed}")
        logger.info(f"Sucessos: {self.success_count}")
        logger.info(f"Falhas: {self.failure_count}")
        logger.info(f"Ignorados: {self.skipped_count}")
        logger.info(f"Duração total: {duration:.2f} segundos")
        logger.info(f"Velocidade: {pages_per_second:.2f} páginas/segundo")
        logger.info("="*50)

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
            
        await self._log_summary()
        logger.info("Monitoramento de recursos finalizado")
    
    async def _monitor_loop(self):
        """Loop principal de monitoramento"""
        while self.running:
            try:
                self._collect_stats()
                self._log_current_stats()
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erro no monitoramento: {str(e)}")
    
    def _collect_stats(self):
        """Coleta estatísticas do sistema"""
        if not HAS_PSUTIL:
            return
            
        try:
            cpu_percent = psutil.cpu_percent(interval=0.5)
            memory = psutil.virtual_memory()
            
            self.stats.append({
                'timestamp': time.time(),
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_used_gb': memory.used / (1024 ** 3)
            })
            
            # Manter apenas dados dos últimos 10 minutos
            cutoff = time.time() - 600
            self.stats = [s for s in self.stats if s['timestamp'] > cutoff]
        except Exception as e:
            logger.error(f"Erro coletando estatísticas: {str(e)}")
    
    def _log_current_stats(self):
        """Registra estatísticas atuais"""
        if not self.stats:
            return
            
        latest = self.stats[-1]
        logger.info(f"Recursos: CPU {latest['cpu_percent']:.1f}% | "
                    f"RAM {latest['memory_percent']:.1f}% ({latest['memory_used_gb']:.2f} GB)")
                    
        # Alertas
        if latest['cpu_percent'] > 90:
            logger.warning("ALERTA: Uso de CPU muito alto (>90%)")
        if latest['memory_percent'] > 90:
            logger.warning("ALERTA: Uso de memória muito alto (>90%)")
    
    async def _log_summary(self):
        """Registra resumo de uso de recursos"""
        if not self.stats:
            return
            
        avg_cpu = sum(s['cpu_percent'] for s in self.stats) / len(self.stats)
        avg_mem = sum(s['memory_percent'] for s in self.stats) / len(self.stats)
        max_cpu = max(s['cpu_percent'] for s in self.stats)
        max_mem = max(s['memory_percent'] for s in self.stats)
        
        logger.info(f"Resumo de recursos - CPU média: {avg_cpu:.1f}% (máx: {max_cpu:.1f}%) | "
                    f"RAM média: {avg_mem:.1f}% (máx: {max_mem:.1f}%)")

class MigrationQueue:
    """Fila de migração com priorização"""
    
    def __init__(self):
        self.high_priority = asyncio.Queue()
        self.normal_priority = asyncio.Queue()
        self.low_priority = asyncio.Queue()
        self.processed_items = set()
    
    async def put(self, item, priority="normal"):
        """Adiciona item à fila com prioridade"""
        item_id = self._get_item_id(item)
        
        # Evita reprocessamento
        if item_id in self.processed_items:
            return False
            
        self.processed_items.add(item_id)
        
        if priority == "high":
            await self.high_priority.put(item)
        elif priority == "normal":
            await self.normal_priority.put(item)
        else:
            await self.low_priority.put(item)
            
        return True
    
    async def get(self):
        """Obtém próximo item respeitando prioridade"""
        # Verificar filas em ordem de prioridade
        if not self.high_priority.empty():
            return await self.high_priority.get()
            
        if not self.normal_priority.empty():
            return await self.normal_priority.get()
            
        if not self.low_priority.empty():
            return await self.low_priority.get()
            
        # Nenhum item encontrado
        return None
    
    async def is_empty(self):
        """Verifica se todas as filas estão vazias"""
        return (self.high_priority.empty() and 
                self.normal_priority.empty() and 
                self.low_priority.empty())
    
    def _get_item_id(self, item):
        """Gera um ID único para o item baseado na URL e título"""
        if isinstance(item, dict):
            if 'url' in item and 'title' in item:
                return f"{item['url']}_{item['title']}"
            elif 'article_id' in item:
                return f"article_{item['article_id']}"
        return str(id(item))

class ContentMigrator:
    """Gerenciador de migração de conteúdo otimizado"""
    
    def __init__(self, config=None):
        self.config = config or Config()
        self.stats = MigrationStats()
        self.monitor = ResourceMonitor()
        self.migration_queue = MigrationQueue()
        self.image_update_queue = MigrationQueue()
        self.document_update_queue = MigrationQueue()
        self.content_mapping = {}
        self.update_results = {}
        self._creator_pool = []
        self._updater_pool = []
        self._semaphore = None
        self._resource_detection_done = False
        self._optimal_concurrent = 20
        self._chunk_size = 10
        
        # Ampliação de caches LRU
        self._enlarge_lru_caches()
    
    def _enlarge_lru_caches(self):
        """Amplia os caches LRU para melhor performance"""
        import functools
        import inspect
        import sys
        
        # Amplia caches existentes
        for name, obj in inspect.getmembers(sys.modules['creators.web_content_creator']):
            if inspect.isfunction(obj) and hasattr(obj, '__wrapped__'):
                if isinstance(obj, functools._lru_cache_wrapper):
                    # Aumenta o tamanho do cache
                    original_func = obj.__wrapped__
                    new_size = getattr(obj, 'maxsize', 128) * 4
                    new_func = functools.lru_cache(maxsize=new_size)(original_func)
                    
                    # Substitui a função original (quando possível)
                    try:
                        setattr(sys.modules['creators.web_content_creator'], name, new_func)
                    except:
                        pass
    
    def detect_optimal_resources(self):
        """Detecta recursos ótimos do sistema para paralelismo"""
        if self._resource_detection_done:
            return self._optimal_concurrent, self._chunk_size
            
        try:
            import multiprocessing
            
            # Obter número de CPUs
            cpu_count = multiprocessing.cpu_count()
            
            if HAS_PSUTIL:
                # Ajustar com base na memória disponível
                mem = psutil.virtual_memory()
                mem_gb = mem.available / (1024**3)  # Memória disponível em GB
                
                # Heurística: 2-5 tasks por CPU dependendo da memória disponível
                cpu_factor = min(5, max(2, int(mem_gb / 2)))
                self._optimal_concurrent = min(50, cpu_count * cpu_factor)
            else:
                # Conservador: 3 tasks por CPU
                self._optimal_concurrent = min(40, cpu_count * 3)
            
            # Chunk size proporcional à concorrência
            self._chunk_size = min(20, max(5, self._optimal_concurrent // 4))
            
            logger.info(f"Configuração automática: {self._optimal_concurrent} tarefas concorrentes, "
                       f"chunks de {self._chunk_size}")
        except Exception as e:
            logger.warning(f"Erro detectando recursos: {str(e)}")
            logger.info("Usando configuração padrão: 20 tarefas concorrentes, chunks de 10")
            self._optimal_concurrent = 20
            self._chunk_size = 10
            
        self._resource_detection_done = True
        return self._optimal_concurrent, self._chunk_size
    
    async def initialize(self, max_concurrent=None):
        """Inicializa o migrador com pool de objetos"""
        if not max_concurrent:
            max_concurrent, _ = self.detect_optimal_resources()
            
        self._semaphore = asyncio.Semaphore(max_concurrent)
        
        # Criar pool de creators (limite para não sobrecarregar)
        pool_size = min(max_concurrent, 8)
        
        logger.info(f"Inicializando pool com {pool_size} creators e updaters")
        
        for i in range(pool_size):
            creator = WebContentCreator(self.config)
            await creator.initialize_session()
            self._creator_pool.append(creator)
            
            updater = ContentUpdater(self.config)
            await updater.initialize_session()
            self._updater_pool.append(updater)
            
        logger.info(f"Pool inicializado com sucesso")
    
    async def cleanup(self):
        """Encerra recursos e conexões"""
        logger.info("Encerrando recursos...")
        
        # Fechar creators
        if self._creator_pool:
            close_tasks = [creator.close() for creator in self._creator_pool]
            await asyncio.gather(*close_tasks, return_exceptions=True)
            
        # Fechar updaters
        if self._updater_pool:
            close_tasks = [updater.close() for updater in self._updater_pool]
            await asyncio.gather(*close_tasks, return_exceptions=True)
            
        # Parar monitoramento
        await self.monitor.stop()
        
        logger.info("Recursos encerrados")
    
    async def _content_migration_worker(self):
        """Worker para migração de conteúdo principal"""
        while True:
            async with self._semaphore:
                # Obter próxima página da fila
                page = await self.migration_queue.get()
                if page is None:
                    break
                
                try:
                    logger.info(f"\nProcessando conteúdo: {page['title']}")
                    logger.info(f"Hierarquia: {' > '.join(page['hierarchy'])}")
                    
                    # Obtém um creator do pool de forma circular
                    worker_id = id(asyncio.current_task()) % len(self._creator_pool)
                    creator = self._creator_pool[worker_id]
                    
                    # Migra conteúdo
                    content_result = await creator.migrate_content(
                        source_url=page['url'],
                        title=page['title'],
                        hierarchy=page['hierarchy']
                    )
                    
                    if content_result:
                        content_id = content_result['id'] if isinstance(content_result, dict) else content_result
                        logger.info(f"Conteúdo migrado: {page['title']} (ID: {content_id})")
                        self.content_mapping[page['title']] = content_id
                        self.stats.content_created += 1
                        self.stats.success_count += 1
                        
                        # Adicionar à fila de processamento de imagens
                        await self.image_update_queue.put({
                            'title': page['title'],
                            'url': page['url'],
                            'content_id': content_id,
                            'hierarchy': page['hierarchy']
                        }, "normal")
                    else:
                        logger.error(f"Falha ao migrar conteúdo: {page['title']}")
                        self.stats.failure_count += 1
                
                except Exception as e:
                    logger.error(f"Erro processando conteúdo {page['title']}: {str(e)}")
                    self.stats.failure_count += 1
    
    async def _image_processor_worker(self):
        """Worker para processamento de imagens"""
        while True:
            async with self._semaphore:
                # Obter próximo item da fila de imagens
                item = await self.image_update_queue.get()
                if item is None:
                    break
                
                try:
                    title = item['title']
                    content_id = item['content_id']
                    
                    logger.info(f"Processando imagens para: {title}")
                    
                    # Obtém um updater do pool de forma circular
                    worker_id = id(asyncio.current_task()) % len(self._updater_pool)
                    updater = self._updater_pool[worker_id]
                    
                    # Envia para processamento de imagens
                    # Aqui seria o código para processar imagens e documentos
                    # Por enquanto vamos apenas simular para demonstração
                    await asyncio.sleep(0.1)  # Simulação
                    
                    # Atualizar estatísticas
                    self.stats.images_processed += 1
                    
                    # Adicionar à fila de documentos
                    await self.document_update_queue.put({
                        'title': title,
                        'content_id': content_id,
                        'url': item['url']
                    }, "normal")
                    
                except Exception as e:
                    logger.error(f"Erro processando imagens para {item['title']}: {str(e)}")
    
    async def _document_processor_worker(self):
        """Worker para processamento de documentos"""
        while True:
            async with self._semaphore:
                # Obter próximo item da fila de documentos
                item = await self.document_update_queue.get()
                if item is None:
                    break
                
                try:
                    title = item['title']
                    content_id = item['content_id']
                    
                    logger.info(f"Processando documentos para: {title}")
                    
                    # Obtém um updater do pool de forma circular
                    worker_id = id(asyncio.current_task()) % len(self._updater_pool)
                    updater = self._updater_pool[worker_id]
                    
                    # Envia para processamento de documentos
                    # Aqui seria o código para processar documentos
                    # Por enquanto vamos apenas simular para demonstração
                    await asyncio.sleep(0.1)  # Simulação
                    
                    # Atualizar estatísticas
                    self.stats.documents_processed += 1
                    
                except Exception as e:
                    logger.error(f"Erro processando documentos para {item['title']}: {str(e)}")
    
    async def migrate_contents(self, pages, max_concurrent=None, chunk_size=None):
        """
        Migrates web content based on the provided pages using multithreading.
        
        Args:
            pages (list): List of page data dictionaries
            max_concurrent (int): Maximum number of concurrent tasks
            chunk_size (int): Size of chunks for batch processing
            
        Returns:
            dict: Dictionary mapping page titles to content IDs
        """
        # Usar detecção automática se não especificado
        if not max_concurrent or not chunk_size:
            max_concurrent, chunk_size = self.detect_optimal_resources()
        
        # Inicializar recursos
        await self.initialize(max_concurrent)
        await self.monitor.start()
        
        # Preparar estatísticas
        self.stats = MigrationStats()
        self.stats.total_pages = len(pages)
        
        try:
            # Adicionar páginas à fila de migração por prioridade
            for page in pages:
                if len(page['hierarchy']) <= 1:
                    priority = "high"
                elif len(page['hierarchy']) > 3:
                    priority = "low"
                else:
                    priority = "normal"
                
                await self.migration_queue.put(page, priority)
            
            logger.info(f"Iniciando migração de {len(pages)} páginas com {max_concurrent} tarefas concorrentes")
            
            # Criar workers para cada etapa do processo
            content_workers = [
                asyncio.create_task(self._content_migration_worker()) 
                for _ in range(max_concurrent)
            ]
            
            image_workers = [
                asyncio.create_task(self._image_processor_worker())
                for _ in range(max(2, max_concurrent // 4))  # Menos workers para imagens
            ]
            
            document_workers = [
                asyncio.create_task(self._document_processor_worker())
                for _ in range(max(2, max_concurrent // 4))  # Menos workers para documentos
            ]
            
            # Aguardar conclusão dos workers de conteúdo
            await asyncio.gather(*content_workers)
            
            # Sinalizar fim da fila de imagens e aguardar conclusão
            for _ in range(len(image_workers)):
                await self.image_update_queue.put(None)
            await asyncio.gather(*image_workers)
            
            # Sinalizar fim da fila de documentos e aguardar conclusão
            for _ in range(len(document_workers)):
                await self.document_update_queue.put(None)
            await asyncio.gather(*document_workers)
            
            # Registrar estatísticas
            self.stats.end_time = time.time()
            self.stats.log_summary()
            
            return self.content_mapping
            
        finally:
            # Limpar recursos
            await self.cleanup()
    
    async def update_contents(self, pages, max_concurrent=None, chunk_size=None):
        """
        Updates existing content based on article IDs in the provided pages.
        
        Args:
            pages (list): List of page data dictionaries with article_id keys
            max_concurrent (int): Maximum number of concurrent updates
            chunk_size (int): Size of chunks for batch processing
            
        Returns:
            dict: Dictionary mapping article IDs to update status (boolean)
        """
        # Usar detecção automática se não especificado
        if not max_concurrent or not chunk_size:
            max_concurrent, chunk_size = self.detect_optimal_resources()
        
        # Inicializar recursos se ainda não estiverem
        if not self._creator_pool:
            await self.initialize(max_concurrent)
            await self.monitor.start()
        
        # Filtrar páginas com article_id válido
        pages_to_update = [
            page for page in pages
            if page.get('article_id', '').strip().isdigit()
        ]
        
        if not pages_to_update:
            logger.warning("Nenhum artigo encontrado para atualização")
            return {}
        
        # Preparar estatísticas
        self.stats.total_pages = len(pages_to_update)
        
        try:
            # Criar semáforo para controle de concorrência
            update_semaphore = asyncio.Semaphore(max_concurrent)
            results = {}
            
            async def update_single_article(page):
                async with update_semaphore:
                    article_id = page['article_id']
                    title = page['title']
                    
                    logger.info(f"\nProcessando artigo: {title}")
                    logger.info(f"ID: {article_id}")
                    
                    try:
                        # Obtém um updater do pool de forma circular
                        worker_id = id(asyncio.current_task()) % len(self._updater_pool)
                        updater = self._updater_pool[worker_id]
                        
                        success = await updater.update_article_content(
                            article_id=article_id,
                            old_url=""  # URL não é necessária para atualização
                        )
                        
                        if success:
                            logger.info(f"✓ Artigo atualizado com sucesso: {title}")
                            self.stats.success_count += 1
                        else:
                            logger.error(f"✗ Falha ao atualizar artigo: {title}")
                            self.stats.failure_count += 1
                        
                        return article_id, success
                            
                    except Exception as e:
                        logger.error(f"Erro processando artigo {article_id}: {str(e)}")
                        self.stats.failure_count += 1
                        return article_id, False
            
            # Processar em lotes para melhor controle de recursos
            chunk_size = min(chunk_size, len(pages_to_update))
            chunks = [pages_to_update[i:i + chunk_size] for i in range(0, len(pages_to_update), chunk_size)]
            
            logger.info(f"Iniciando atualização de {len(pages_to_update)} artigos em {len(chunks)} lotes")
            
            for i, chunk in enumerate(chunks):
                logger.info(f"Processando lote {i+1}/{len(chunks)}")
                
                # Criar tasks para cada artigo no lote
                tasks = [update_single_article(page) for page in chunk]
                chunk_results = await asyncio.gather(*tasks)
                
                # Processar resultados
                for result in chunk_results:
                    if result:
                        article_id, success = result
                        results[article_id] = success
            
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
            
            return results
            
        finally:
            # Limpar recursos se não foi iniciado de migrate_contents
            if not self.content_mapping:
                await self.cleanup()

# Funções exportadas mantendo a assinatura original
async def migrate_contents(pages):
    """
    Migrates web content based on the provided pages.
    
    Args:
        pages (list): List of page data dictionaries
        
    Returns:
        dict: Dictionary mapping page titles to content IDs
    """
    migrator = ContentMigrator()
    return await migrator.migrate_contents(pages)

async def update_contents(pages):
    """
    Updates existing content based on article IDs in the provided pages.
    
    Args:
        pages (list): List of page data dictionaries with article_id keys
        
    Returns:
        dict: Dictionary mapping article IDs to update status (boolean)
    """
    migrator = ContentMigrator()
    return await migrator.update_contents(pages)

# Funções adicionais para uso avançado

async def migrate_contents_advanced(pages, max_concurrent=None, chunk_size=None, 
                                  monitor_resources=True, batch_size=200):
    """
    Versão avançada da migração com múltiplas opções
    
    Args:
        pages (list): Lista de dicionários com dados das páginas
        max_concurrent (int): Número máximo de tarefas concorrentes
        chunk_size (int): Tamanho dos lotes para processamento
        monitor_resources (bool): Ativa monitoramento de recursos
        batch_size (int): Tamanho do lote principal para processamento em etapas
        
    Returns:
        dict: Dicionário mapeando títulos de páginas para IDs de conteúdo
    """
    # Processamento em lotes grandes para evitar problemas de memória
    if len(pages) > batch_size:
        migrator = ContentMigrator()
        if not max_concurrent:
            max_concurrent, chunk_size = migrator.detect_optimal_resources()
            
        logger.info(f"Migrando {len(pages)} páginas em lotes de {batch_size}")
        
        result = {}
        batches = [pages[i:i + batch_size] for i in range(0, len(pages), batch_size)]
        
        for i, batch in enumerate(batches):
            logger.info(f"Processando lote {i+1}/{len(batches)} ({len(batch)} páginas)")
            
            batch_result = await migrator.migrate_contents(batch, max_concurrent, chunk_size)
            result.update(batch_result)
            
            if i < len(batches) - 1:
                # Pausa entre lotes para liberar recursos
                logger.info("Pausa para liberação de recursos...")
                await asyncio.sleep(5)
                
        return result
    else:
        # Para conjuntos menores, usar migração normal
        migrator = ContentMigrator()
        return await migrator.migrate_contents(pages, max_concurrent, chunk_size)

def get_optimal_concurrency():
    """
    Determina os valores ótimos de concorrência com base no ambiente de execução
    
    Returns:
        tuple: (max_concurrent, chunk_size)
    """
    migrator = ContentMigrator()
    return migrator.detect_optimal_resources()