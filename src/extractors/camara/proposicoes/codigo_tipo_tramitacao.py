from extractors.camara.base import CamaraBaseExtractor

class CodigoTipoTramitacaoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/proposicoes/codTipoTramitacao'

    def extract(self):
        try:
            response = self.client.get(self.ENDPOINT)
            data = response.get('dados', [])
            return data
        
        except Exception as e:
            print(f'Error while extracting codigo tipo tramitacao: {e}')
            return []