from clients.camara_client import CamaraClient

class CamaraBaseExtractor:
    def __init__(self, client: CamaraClient):
        self.client = client
    
    def extract(self):
        raise NotImplementedError('The extract method must be implemented by subclasses.')