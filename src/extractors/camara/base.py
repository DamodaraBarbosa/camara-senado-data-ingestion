from clients.camara_client import AsyncCamaraClient


class CamaraBaseExtractor:
    def __init__(self, client: AsyncCamaraClient, params: dict = None, bulk_client=None):
        self.client = client
        self.params = params or {}
        self._bulk_client = bulk_client
        # Extractors sinalizam entrega parcial aqui; os runners leem via getattr
        # e reportam status "partial".
        self.partial = False

    @property
    def bulk(self):
        """Cliente dos arquivos bulk, instanciado sob demanda.

        Lazy para que os extractors que só usam a API não paguem por ele, e
        injetável para que os testes não toquem a rede.
        """
        if self._bulk_client is None:
            from clients.camara_bulk_client import CamaraBulkClient
            self._bulk_client = CamaraBulkClient()
        return self._bulk_client

    async def extract(self):
        raise NotImplementedError('The extract method must be implemented by subclasses.')
