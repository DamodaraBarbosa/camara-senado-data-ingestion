from extractors.camara.base import CamaraBaseExtractor

class CodigoSituacaoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/deputados/codSituacao'

    def extract(self):
        response = self.client.get(self.ENDPOINT)
        data = response.get('dados', [])
        return data