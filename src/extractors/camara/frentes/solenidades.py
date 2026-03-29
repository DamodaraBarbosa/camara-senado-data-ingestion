from extractors.camara.base import AsyncBaseExtractor

class CamaraBaseExtractor(AsyncBaseExtractor):
    def __init__(self, client):
        self.client = client
        self.ENDPOINT = None
        self.LEGISLATURAS = None 

        async def extract(self, **kwargs):
            if self.ENDPOINT is None:
                raise NotImplementedError("ENDPOINT must be defined in the subclass.")
            if self.LEGISLATURAS is None:
                raise NotImplementedError("LEGISLATURAS must be defined in the subclass.")

            data = []
            for legislatura in self.LEGISLATURAS:
                params = {'legislatura': legislatura}
                params.update(kwargs)
                response = await self.client.get(self.ENDPOINT, params=params)
                data.extend(response['dados'])
            return data
        
        return extract