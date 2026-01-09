import requests

class CamaraClient:
    def __init__(self, url='https://dadosabertos.camara.leg.br/api/v2/'):
        self.url = url

    def get(self, endpoint: str):
        if not endpoint:
            raise ValueError('The endpoint parameter must be not empty.')
        
        url = f'{self.url}{endpoint}'
        request = requests.get(url)

        return request.json()