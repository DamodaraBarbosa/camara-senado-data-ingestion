from extractors.camara.base import CamaraBaseExtractor
from datetime import datetime
import aiohttp
import asyncio
import logging

logger = logging.getLogger(__name__)


class AsyncProposicoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes'
    LEGISLATURA = 'legislaturas'
    REQUEST_TIMEOUT = 30  # seconds
    MAX_CONSECUTIVE_EMPTY_PAGES = 4

    async def extract(
        self,
        autor: str = None,
        init_legislatura: int = None,
        sigla_partido_autor: list = None,
        sigla_uf_autor: list = None,
        tramitacao_senado: bool = None,
        itens: int = 100,
        max_consecutive_empty_pages: int = None,
        start_year: int = None
    ):
        """
        Extract propositions from Câmara API with optimized parallel requests.
        
        Args:
            autor: Author filter
            init_legislatura: Initial legislature ID (for fetching start year)
            sigla_partido_autor: Party sigla filter(s)
            sigla_uf_autor: State sigla filter(s)
            tramitacao_senado: Senate processing filter
            itens: Items per page (default 100)
            max_consecutive_empty_pages: Max consecutive empty pages before stopping (default 4)
            start_year: Override start year (useful if legislature is already cached)
            
        Returns:
            List of propositions
        """
        max_consecutive_empty_pages = max_consecutive_empty_pages or self.MAX_CONSECUTIVE_EMPTY_PAGES
        
        timeout = aiohttp.ClientTimeout(total=self.REQUEST_TIMEOUT)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Determine year range
            if start_year is None:
                start_year = await self._get_start_year(session, init_legislatura)
            
            current_year = datetime.now().year
            years_range = range(start_year, current_year + 1)
            
            logger.info(f'Extracting propositions from {start_year} to {current_year}')
            
            # Create tasks for each year (parallel execution)
            year_tasks = [
                self._extract_year(
                    session,
                    ano,
                    autor,
                    sigla_partido_autor,
                    sigla_uf_autor,
                    tramitacao_senado,
                    itens,
                    max_consecutive_empty_pages
                )
                for ano in years_range
            ]
            
            # Execute all year extractions in parallel
            year_results = await asyncio.gather(*year_tasks, return_exceptions=True)
            
            # Aggregate results and handle errors
            all_proposicoes = []
            for ano, result in zip(years_range, year_results):
                if isinstance(result, Exception):
                    logger.error(f'Error extracting year {ano}: {result}')
                    continue
                
                all_proposicoes.extend(result)
            
            logger.info(f'Total propositions extracted: {len(all_proposicoes)}')
            return all_proposicoes

    async def _get_start_year(self, session: aiohttp.ClientSession, init_legislatura: int = None) -> int:
        """
        Fetch the start year from legislature data.
        
        Args:
            session: aiohttp session
            init_legislatura: Legislature ID
            
        Returns:
            Start year as integer
        """
        try:
            params = {}
            if init_legislatura is not None:
                params['id'] = init_legislatura
            
            legislatura = await self.client.get(session, self.LEGISLATURA, params=params)
            
            if not legislatura.get('dados'):
                logger.warning('No legislature data found, using current year')
                return datetime.now().year
            
            start_legislatura_date = legislatura['dados'][0].get('dataInicio')
            if not start_legislatura_date:
                logger.warning('No start date in legislature data, using current year')
                return datetime.now().year
            
            start_year = int(start_legislatura_date.split('-')[0])
            logger.info(f'Legislature start year: {start_year}')
            return start_year
            
        except Exception as e:
            logger.error(f'Error fetching legislature: {e}, using current year')
            return datetime.now().year

    async def _extract_year(
        self,
        session: aiohttp.ClientSession,
        year: int,
        autor: str,
        sigla_partido_autor: list,
        sigla_uf_autor: list,
        tramitacao_senado: bool,
        itens: int,
        max_consecutive_empty_pages: int
    ) -> list:
        """
        Extract all propositions for a specific year with pagination.
        
        Args:
            session: aiohttp session
            year: Year to extract
            Other args: filter parameters
            max_consecutive_empty_pages: Stop after N empty pages
            
        Returns:
            List of propositions for the year
        """
        year_data = []
        page = 1
        consecutive_empty_pages = 0
        
        logger.debug(f'Starting extraction for year {year}')
        
        while consecutive_empty_pages < max_consecutive_empty_pages:
            try:
                params = {
                    'autor': autor,
                    'ano': year,
                    'siglaPartidoAutor': sigla_partido_autor,
                    'siglaUfAutor': sigla_uf_autor,
                    'tramitacaoSenado': tramitacao_senado,
                    'itens': itens,
                    'pagina': page
                }
                
                # Remove None values
                params = {k: v for k, v in params.items() if v is not None}
                
                response = await self.client.get(session, self.ENDPOINT, params=params)
                data = response.get('dados', [])
                
                logger.debug(f'Year {year}, page {page}: {len(data)} propositions')
                
                if not data:
                    consecutive_empty_pages += 1
                    page += 1
                    continue

                print(f'Year {year}, page {page}: {len(data)} propositions')  # Progress indicator
                
                consecutive_empty_pages = 0  # Reset counter on successful fetch
                year_data.extend(data)
                page += 1
                
            except asyncio.TimeoutError:
                logger.error(f'Timeout extracting year {year}, page {page}')
                consecutive_empty_pages += 1
                page += 1
            except Exception as e:
                logger.error(f'Error extracting year {year}, page {page}: {e}')
                consecutive_empty_pages += 1
                page += 1
        
        logger.info(f'Year {year} complete: {len(year_data)} propositions')
        return year_data
