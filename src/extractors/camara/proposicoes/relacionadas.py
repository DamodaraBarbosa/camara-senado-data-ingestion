from extractors.camara.base import CamaraBaseExtractor
import json
import aiohttp

class AsyncRelacionadasExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/relacionadas'

    async def extract(
            self, 
            proposicoes: json
        ):
        session = aiohttp.ClientSession()
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id') for proposicao in proposicoes if proposicao.get('id')))
        all_relacionadas = []

        for proposicao_id in proposicoes_ids:
            try:
                response = await self.client.get(session, self.ENDPOINT.format(id=proposicao_id))
                data = response.get('dados', [])

                for relacionada in data:
                    relacionada['relacionadoProposicao'] = proposicao_id

                all_relacionadas.extend(data)

            except Exception as e:
                print(f'Error while extracting relacionadas for proposicao {proposicao_id}: {e}')

        await session.close()
        return all_relacionadas