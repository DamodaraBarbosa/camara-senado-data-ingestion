import asyncio
import aiohttp
from urllib.parse import urlparse, parse_qs
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


# Configuração otimizada:
# - total: 90s para cada requisição individual
# - connect: 30s para estabelecer conexão
_TIMEOUT = aiohttp.ClientTimeout(total=90, connect=30)

# Status codes que justificam retry
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# Limite de tempo total para uma operação de extração (evita loops infinitos)
# Se exceder esse tempo mesmo com retries, a task falha e Airflow faz retry
_MAX_OPERATION_TIME = 600  # 10 minutos


class AsyncCamaraClient:
    def __init__(self, url='https://dadosabertos.camara.leg.br/api/v2/'):
        self.url = url
        self._semaphore = None

    @property
    def semaphore(self):
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(15)
        return self._semaphore

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    )
    async def get(self, session: aiohttp.ClientSession, endpoint: str, params: dict = None):
        if not endpoint:
            raise ValueError('The endpoint parameter must be not empty.')

        url = f'{self.url}{endpoint}'

        async with self.semaphore:
            async with session.get(url, params=params, timeout=_TIMEOUT) as response:
                if response.status == 404:
                    return {}

                if response.status in _RETRYABLE_STATUSES:
                    print(f'[client] HTTP {response.status} — retry decorator will handle exponential backoff.')
                    raise aiohttp.ClientError(f"HTTP {response.status}: {response.reason}")

                response.raise_for_status()

                text = await response.text()
                if not text:
                    return {}

                return await response.json()

    async def get_all_pages(self, session: aiohttp.ClientSession, endpoint: str, params: dict = None, itens: int = 100):
        """
        Fetch all pages of a paginated endpoint in parallel using links metadata.

        Args:
            session: aiohttp ClientSession
            endpoint: API endpoint (e.g., 'deputados')
            params: Query parameters (optional)
            itens: Items per page (default 100)

        Returns:
            Combined list of all records from all pages
        """
        # Start with page 1 to discover total pages
        page1_params = {**(params or {}), 'itens': itens, 'pagina': 1}
        page1_params = {k: v for k, v in page1_params.items() if v is not None}

        page1_response = await self.get(session, endpoint, params=page1_params)
        all_data = page1_response.get('dados', [])

        # Extract total pages from links metadata
        links = page1_response.get('links', [])
        last_page = 1

        for link in links:
            if link.get('rel') == 'last':
                href = link.get('href', '')
                # Parse the href to extract pagina param
                parsed = urlparse(href)
                query_params = parse_qs(parsed.query)
                if 'pagina' in query_params:
                    try:
                        last_page = int(query_params['pagina'][0])
                    except (ValueError, IndexError):
                        pass
                break

        # If there are more pages, fetch them in parallel
        if last_page > 1:
            tasks = []
            for page_num in range(2, last_page + 1):
                page_params = {**(params or {}), 'itens': itens, 'pagina': page_num}
                page_params = {k: v for k, v in page_params.items() if v is not None}
                task = self.get(session, endpoint, params=page_params)
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            for result in results:
                page_data = result.get('dados', [])
                all_data.extend(page_data)

        return all_data
