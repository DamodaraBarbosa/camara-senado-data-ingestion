from extractors.camara.base import CamaraBaseExtractor

class TiposProposicaoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/tiposProposicao'

    def extract(self):
        try:
            response = self.client.get(self.ENDPOINT)
            data = response.get('dados', [])

            return data
        
        except Exception as e:
            print(f'Error while extracting tipos proposicao: {e}')
            return []