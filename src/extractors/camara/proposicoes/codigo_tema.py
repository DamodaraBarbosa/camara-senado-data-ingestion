from extractors.camara.base import CamaraBaseExtractor

class CodigoTemaExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/proposicoes/codTema'

    def extract(self):
        try:
            response = self.client.get(self.ENDPOINT)
            data = response.get('dados', [])
            return data
        
        except Exception as e:
            print(f'Error while extracting codigo tema: {e}')
            return []