import logging
import asyncio
from configs.config import Config
from creators.page_creator import PageCreator
import aiohttp
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from collections import OrderedDict

logger = logging.getLogger(__name__)

class MigrationStats:
    def __init__(self):
        self.total_pages = 0
        self.successful_pages = 0
        self.failed_pages = []
        self.start_time = None
        self.end_time = None
        
    def start(self, total_pages: int):
        self.total_pages = total_pages
        self.start_time = datetime.now()
        
    def record_success(self, page_title: str):
        self.successful_pages += 1
        
    def record_failure(self, page_title: str, error: str):
        self.failed_pages.append((page_title, error))
        
    def finish(self):
        self.end_time = datetime.now()
        
    @property
    def duration(self) -> float:
        if not (self.start_time and self.end_time):
            return 0
        return (self.end_time - self.start_time).total_seconds()
    
    def get_summary(self) -> str:
        success_rate = (self.successful_pages / self.total_pages * 100) if self.total_pages > 0 else 0
        
        summary = [
            "\n=== Resumo da Migração ===",
            f"Total de páginas processadas: {self.total_pages}",
            f"Páginas criadas com sucesso: {self.successful_pages}",
            f"Páginas com falha: {len(self.failed_pages)}",
            f"Taxa de sucesso: {success_rate:.1f}%",
            f"Tempo total de execução: {self.duration:.1f} segundos",
            f"Média de tempo por página: {(self.duration / self.total_pages):.1f} segundos",
        ]
        
        if self.failed_pages:
            summary.extend([
                "\nPáginas que falharam:",
                *[f"- {title}: {error}" for title, error in self.failed_pages]
            ])
            
        return "\n".join(summary)

class PageMigrator:
    def __init__(self, config: Config, batch_size: int = 3):
        self.config = config
        self.creator = PageCreator(config)
        self.session: Optional[aiohttp.ClientSession] = None
        self.page_mapping: Dict[str, str] = {}
        self.stats = MigrationStats()
        self.batch_size = batch_size
        self.semaphore = asyncio.Semaphore(batch_size)
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(
                login=self.config.liferay_user,
                password=self.config.liferay_pass
            ),
            headers={"Content-Type": "application/json"},
            connector=aiohttp.TCPConnector(
                ssl=False,
                limit=100
            ),
            timeout=aiohttp.ClientTimeout(total=300)
        )
        self.creator.session = self.session
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def process_page(self, page: Dict, current: int, total: int) -> Tuple[bool, Optional[str]]:
        """Processa uma única página e retorna o status"""
        async with self.semaphore:
            try:
                logger.info(f"\nProcessando página {current}/{total}: {page['title']}")
                logger.info(f"Hierarquia: {' > '.join(page['hierarchy'])}")
                logger.info(f"Tipo de pagina: {page['type']}")

                needs_menu = page['column_type'] == '2_columns_ii'
                menu_title = page.get('menu_title') if needs_menu else None
                final_url = page['url'].strip('/').split('/')[-1] if page['url'] else ''

                page_id = await self.creator.create_hierarchy(
                    hierarchy=page['hierarchy'],
                    final_title=page['title'],
                    final_url=final_url,
                    page_type=page['type'],
                    visible=page['visible'],
                    column_type=page['column_type'],
                    menu_title=menu_title,
                    url_vinculada=page['url_vincular']
                )

                if page_id:
                    self.page_mapping[page['title']] = page_id
                    logger.info(f"✓ Página criada: {page['title']} (ID: {page_id})")
                    return True, None
                else:
                    error_msg = "Falha na criação da página"
                    logger.error(f"✗ {error_msg}: {page['title']}")
                    return False, error_msg

            except Exception as e:
                error_msg = str(e)
                logger.error(f"✗ Erro ao processar página {page['title']}: {error_msg}")
                return False, error_msg

    async def process_batch(self, batch: List[Dict], start_idx: int, total_pages: int) -> None:
        """Processa um lote de páginas em paralelo"""
        tasks = []
        for i, page in enumerate(batch, start_idx):
            task = self.process_page(page, i, total_pages)
            tasks.append(task)
            
        results = await asyncio.gather(*tasks)
        
        for (success, error), page in zip(results, batch):
            if success:
                self.stats.record_success(page['title'])
            else:
                self.stats.record_failure(page['title'], error or "Erro desconhecido")

    def organize_pages_by_level(self, pages: List[Dict]) -> OrderedDict:
        """Organiza páginas por nível hierárquico mantendo a ordem original dentro de cada nível"""
        pages_by_level = OrderedDict()
        
        for page in pages:
            level = len(page['hierarchy'])
            if level not in pages_by_level:
                pages_by_level[level] = []
            pages_by_level[level].append(page)
            
        return pages_by_level

    async def process_level(self, level_pages: List[Dict], current_count: int, total_pages: int) -> int:
        """Processa todas as páginas de um nível em batches"""
        logger.info(f"\n=== Processando nível com {len(level_pages)} páginas ===")
        
        for i in range(0, len(level_pages), self.batch_size):
            batch = level_pages[i:i + self.batch_size]
            await self.process_batch(batch, current_count + i, total_pages)
            
            # Mostra progresso
            progress = ((current_count + i + len(batch)) / total_pages) * 100
            logger.info(f"Progresso geral: {progress:.1f}% ({current_count + i + len(batch)}/{total_pages})")
            
        return current_count + len(level_pages)

    async def migrate_pages(self, pages: List[Dict]) -> Dict[str, str]:
        """
        Migra as páginas por nível hierárquico, processando em batches dentro de cada nível
        
        Args:
            pages: Lista de dicionários com dados das páginas
            
        Returns:
            Dict[str, str]: Mapeamento de títulos para IDs das páginas
        """
        try:
            self.stats.start(len(pages))
            
            # Organiza páginas por nível
            pages_by_level = self.organize_pages_by_level(pages)
            current_count = 0
            
            # Processa cada nível sequencialmente
            for level in sorted(pages_by_level.keys()):
                level_pages = pages_by_level[level]
                logger.info(f"\n=== Iniciando processamento do nível {level} ===")
                current_count = await self.process_level(level_pages, current_count, len(pages))
                
            self.stats.finish()
            logger.info(self.stats.get_summary())
            return self.page_mapping

        except Exception as e:
            logger.error(f"Erro durante migração: {str(e)}")
            raise

async def migrate_pages(pages: List[Dict], batch_size: int = 3) -> Dict[str, str]:
    """Função wrapper para manter compatibilidade com código existente"""
    config = Config()
    async with PageMigrator(config, batch_size=batch_size) as migrator:
        return await migrator.migrate_pages(pages)