from extractors.camara.base import CamaraBaseExtractor

class MesaExtractor(CamaraBaseExtractor):
    ENDPOINT = 'legislaturas/{id}/mesa'
    LEGISLATURAS = 'legislaturas'

    def extract(self, init_legislatura: int = None):  
        legislaturas = self.client.get(self.LEGISLATURAS)['dados']
        current_legislatura = max(legislatura['id'] for legislatura in legislaturas)
        
        start = init_legislatura if init_legislatura is not None else current_legislatura
        all_mesa = []

        try:
            for legislatura in range(start, current_legislatura + 1):
                response = self.client.get(self.ENDPOINT.format(id=legislatura))
                print(f'Response: {response}')
                data = response.get('dados', [])
                
                print(f'Legislatura ID: {legislatura}, Data Length: {len(data)}')
                all_mesa.extend(data)
                
        except Exception as e:
            print(f'Error while extracting mesa for legislatura {legislatura}: {e}')
        
        return all_mesa