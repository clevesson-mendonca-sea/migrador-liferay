import asyncio
import logging
from configs.config import Config
from creators.web_content_creator import WebContentCreator
from updaters.content_update import ContentUpdater
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from collections import defaultdict
import asyncio
from asyncio import Queue

logger = logging.getLogger(__name__)

@dataclass
class ContentMigrationStats:
    total_items: int = 0
    processed_items: int = 0
    successful_items: int = 0
    failed_items: List[Tuple[str, str, str]] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    level_stats: Dict[int, Dict[str, int]] = field(default_factory=lambda: defaultdict(lambda: {'total': 0, 'success': 0}))

    def start(self, total_items: int):
        """Inicia a contagem de estatísticas"""
        self.total_items = total_items
        self.start_time = datetime.now()

    def finish(self):
        """Finaliza a contagem de estatísticas"""
        self.end_time = datetime.now()

    def record_success(self, title: str, content_id: str, level: int = 0):
        """Registra um sucesso"""
        self.successful_items += 1
        self.processed_items += 1
        self.level_stats[level]['success'] += 1

    def record_failure(self, title: str, content_id: str, error: str, level: int = 0):
        """Registra uma falha"""
        self.failed_items.append((title, content_id, error))
        self.processed_items += 1

    def add_to_level(self, level: int):
        """Adiciona uma página ao total do nível"""
        self.level_stats[level]['total'] += 1

    @property
    def duration(self) -> float:
        """Retorna a duração total do processamento"""
        if not (self.start_time and self.end_time):
            return 0
        return (self.end_time - self.start_time).total_seconds()

    def get_summary(self) -> str:
        """Retorna um resumo das estatísticas"""
        success_rate = (self.successful_items / self.total_items * 100) if self.total_items > 0 else 0
        
        summary = [
            "\n=== Resumo da Migração ===",
            f"Total processado: {self.processed_items}/{self.total_items}",
            f"Sucesso: {self.successful_items} ({success_rate:.1f}%)",
            f"Falhas: {len(self.failed_items)}",
            f"Tempo total: {self.duration:.1f}s",
            f"Média por item: {(self.duration/self.total_items if self.total_items > 0 else 0):.1f}s",
            "\nEstatísticas por nível:"
        ]

        for level, stats in sorted(self.level_stats.items()):
            if stats['total'] > 0:
                level_success_rate = (stats['success'] / stats['total'] * 100)
                summary.append(f"Nível {level}: {stats['success']}/{stats['total']} ({level_success_rate:.1f}%)")

        if self.failed_items:
            summary.extend([
                "\nFalhas:",
                *[f"- {title} ({id}): {error}" for title, id, error in self.failed_items]
            ])

        return "\n".join(summary)

class ContentMigrator:
    def __init__(self, config: Config, max_concurrent: int = 20):
        self.config = config
        self.creator = WebContentCreator(config)
        self.stats = ContentMigrationStats()
        self.content_mapping: Dict[str, str] = {}
        self.processed_paths: Set[str] = set()
        self.processing_paths: Set[str] = set()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.page_queue: Queue = Queue()
        self.pending_children: Dict[str, List[Dict]] = defaultdict(list)
        self.tasks: Set[asyncio.Task] = set()

    def get_path_key(self, hierarchy: List[str]) -> str:
        """Gera uma chave única para a hierarquia"""
        return '/'.join(hierarchy)

    def get_parent_path(self, hierarchy: List[str]) -> str:
        """Retorna o caminho do pai"""
        return '/'.join(hierarchy[:-1]) if hierarchy else ''

    async def process_content(self, page: Dict[str, Any]) -> Tuple[bool, str]:
        """Processa um único conteúdo"""
        path = self.get_path_key(page['hierarchy'])
        level = len(page['hierarchy'])
        parent_path = self.get_parent_path(page['hierarchy'])

        # Verifica se o pai já foi processado (se não for raiz)
        if parent_path and parent_path not in self.processed_paths:
            # Se o pai não foi processado, coloca na fila de pendentes
            self.pending_children[parent_path].append(page)
            return False, ""

        async with self.semaphore:
            try:
                logger.info(f"\nProcessando: {page['title']} (Nível {level})")
                
                content_id = await self.creator.migrate_content(
                    source_url=page['url'],
                    title=page['title'],
                    hierarchy=page['hierarchy']
                )

                if content_id:
                    self.content_mapping[page['title']] = content_id
                    self.processed_paths.add(path)
                    
                    # Processa filhos pendentes
                    await self.process_pending_children(path)
                    
                    return True, str(content_id)
                
                return False, ""

            except Exception as e:
                logger.error(f"Erro processando {page['title']}: {str(e)}")
                return False, str(e)

    async def process_pending_children(self, parent_path: str):
        """Processa os filhos pendentes de um caminho que acabou de ser concluído"""
        if parent_path in self.pending_children:
            children = self.pending_children.pop(parent_path)
            for child in children:
                await self.add_to_queue(child)

    async def add_to_queue(self, page: Dict[str, Any]):
        """Adiciona uma página à fila de processamento"""
        await self.page_queue.put(page)

    async def process_queue(self):
        """Processa páginas da fila continuamente"""
        while True:
            try:
                page = await self.page_queue.get()
                level = len(page['hierarchy'])

                success, result = await self.process_content(page)
                
                if success:
                    self.stats.record_success(page['title'], result, level)
                    logger.info(f"✓ Sucesso: {page['title']} (ID: {result})")
                else:
                    if not self.get_parent_path(page['hierarchy']) or result:  # Se não tem pai ou deu erro real
                        self.stats.record_failure(page['title'], '', result or "Falha desconhecida", level)
                        logger.error(f"✗ Falha: {page['title']} - {result}")

                self.page_queue.task_done()

            except Exception as e:
                logger.error(f"Erro no processamento da fila: {str(e)}")
                self.page_queue.task_done()

    async def start_workers(self, num_workers: int):
        """Inicia workers para processar a fila"""
        for _ in range(num_workers):
            task = asyncio.create_task(self.process_queue())
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

    async def migrate_contents(self, pages: List[Dict[str, Any]]) -> Dict[str, str]:
        """Migra conteúdos com máximo paralelismo possível"""
        try:
            logger.info(f"Iniciando migração de {len(pages)} páginas")
            self.stats.start(len(pages))

            # Inicia os workers
            await self.start_workers(20)  # Número de workers concorrentes

            # Adiciona todas as páginas à fila
            for page in pages:
                level = len(page['hierarchy'])
                self.stats.add_to_level(level)
                await self.add_to_queue(page)

            # Aguarda conclusão
            await self.page_queue.join()

            # Cancela workers
            for task in self.tasks:
                task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)

            self.stats.finish()
            logger.info(self.stats.get_summary())
            return self.content_mapping

        except Exception as e:
            logger.error(f"Erro durante migração: {str(e)}")
            raise

    async def __aenter__(self):
        await self.creator.initialize_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.creator.close()

async def migrate_contents(pages: List[Dict[str, Any]], max_concurrent: int = 20) -> Dict[str, str]:
    """Função wrapper para migração de conteúdo"""
    config = Config()
    async with ContentMigrator(config, max_concurrent=max_concurrent) as migrator:
        return await migrator.migrate_contents(pages)
    
async def update_contents(pages: List[Dict[str, Any]]) -> Dict[str, bool]:
    """Função wrapper para atualização de conteúdo"""
    config = Config()
    async with ContentUpdater(config) as updater:
        return await updater.update_contents(pages)