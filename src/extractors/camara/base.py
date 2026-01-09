from clients.camara_client import CamaraClient

class CamaraBaseExtractor:
    def __init__(self, client: CamaraClient, params: dict = None):
        self.client = client
        self.params = params or {}

    def extract(self):
        raise NotImplementedError('The extract method must be implemented by subclasses.')