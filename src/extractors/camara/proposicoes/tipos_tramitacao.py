from extractors.camara.base import CamaraBaseExtractor

class TiposTramitacaoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/tiposTramitacao'

    def extract(self):
        try:
            response = self.client.get(self.ENDPOINT)
            data = response.get('dados', [])
            return data
        
        except Exception as e:
            print(f'Error while extracting tipos tramitacao: {e}')
            return []