from extractors.camara.base import CamaraBaseExtractor

class SiglaTipoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/proposicoes/siglaTipo'

    def extract(self):
        try:
            response = self.client.get(self.ENDPOINT)
            data = response.get('dados', [])
            return data
        
        except Exception as e:
            print(f'Error while extracting sigla tipo: {e}')
            return []