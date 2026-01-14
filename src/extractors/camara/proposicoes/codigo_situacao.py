from extractors.camara.base import CamaraBaseExtractor

class CodigoSituacaoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/codigos-situacao'

    def extract(self):
        try:
            response = self.client.get(self.ENDPOINT)
            data = response.get('dados', [])
            return data
        
        except Exception as e:
            print(f'Error while extracting codigo situacao: {e}')
            return []