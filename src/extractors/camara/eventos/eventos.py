from extractors.camara.base import CamaraBaseExtractor
import aiohttp


class AsyncEventosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'eventos'
    LEGISLATURAS = 'legislaturas'

    async def extract(
        self,
        init_legislatura: int = None,
        itens: int = 100,
        request_tries: int = 4
    ):
        all_eventos = []

        async with aiohttp.ClientSession() as session:
            legislaturas = await self.client.get(session, self.LEGISLATURAS)
            legislaturas_data = legislaturas.get('dados', [])
            legislaturas_by_id = {leg['id']: leg for leg in legislaturas_data}
            current_legislatura = max(legislaturas_by_id) if legislaturas_by_id else 0

            start = init_legislatura if init_legislatura is not None else current_legislatura

            for id_legislatura in range(start, current_legislatura + 1):
                # Sem dataInicio/dataFim, /eventos usa o default da API (uma janela
                # estreita de eventos correntes, nao o historico) e devolve so um
                # punhado de registros em vez dos ~11 mil da legislatura inteira
                # (medido em 2026-10-04). Delimitar pelas datas da propria legislatura
                # e o que torna isto um full snapshot de fato.
                legislatura = legislaturas_by_id.get(id_legislatura, {})
                params = {
                    'dataInicio': legislatura.get('dataInicio'),
                    'dataFim': legislatura.get('dataFim'),
                }

                eventos_data = await self.client.get_all_pages(
                    session, self.ENDPOINT, params=params, itens=itens
                )

                for evento in eventos_data:
                    evento['idLegislatura'] = id_legislatura

                all_eventos.extend(eventos_data)

        return all_eventos
