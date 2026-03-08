from extractors.camara.base import CamaraBaseExtractor
import json
import aiohttp

class AsyncTemasExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/temas'

    async def extract(
            self, 
            proposicoes: json
        ):
        session = aiohttp.ClientSession()
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id') for proposicao in proposicoes if proposicao.get('id')))
        all_temas = []

        for proposicao_id in proposicoes_ids:
            try:
                response = await self.client.get(session, self.ENDPOINT.format(id=proposicao_id))
                data = response.get('dados', [])

                for tema in data:
                    tema['idProposicao'] = proposicao_id

                all_temas.extend(data)

            except Exception as e:
                print(f'Error while extracting temas for proposicao {proposicao_id}: {e}')

        await session.close()
        return all_temas