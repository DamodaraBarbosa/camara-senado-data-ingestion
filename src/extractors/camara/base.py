from clients.camara_client import AsyncCamaraClient

class CamaraBaseExtractor:
    def __init__(self, client: AsyncCamaraClient, params: dict = None):
        self.client = client
        self.params = params or {}

    def extract(self):
        raise NotImplementedError('The extract method must be implemented by subclasses.')