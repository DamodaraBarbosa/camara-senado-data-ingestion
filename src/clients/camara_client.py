import requests
from tenacity import retry, stop_after_attempt, wait_exponential

class CamaraClient:
    def __init__(self, url='https://dadosabertos.camara.leg.br/api/v2/'):
        self.url = url

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=10)
    )

    def get(self, endpoint: str, params: dict = None):
        if not endpoint:
            raise ValueError('The endpoint parameter must be not empty.')
        
        url = f'{self.url}{endpoint}'
        response = requests.get(url, params=params, timeout=10)
        
        response.raise_for_status()

        if not response.text:
            return {}

        return response.json()