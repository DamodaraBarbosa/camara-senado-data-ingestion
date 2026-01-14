from extractors.camara.base import CamaraBaseExtractor

class CodigoTipoAutorExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/proposicoes/codTipoAutor'

    def extract(self):
        try:
            response = self.client.get(self.ENDPOINT)
            data = response.get('dados', [])
            return data
        
        except Exception as e:
            print(f'Error while extracting codigo tipo autor: {e}')
            return []