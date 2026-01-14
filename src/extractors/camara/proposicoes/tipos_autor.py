from extractors.camara.base import CamaraBaseExtractor

class TiposAutorExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/tiposAutor'

    def extract(self):
        try:
            response = self.client.get(self.ENDPOINT)
            data = response.get('dados', [])
            return data
        
        except Exception as e:
            print(f'Error while extracting tipos autor: {e}')
            return []