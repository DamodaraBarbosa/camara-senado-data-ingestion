# Câmara-Senado Data Ingestion

A Python-based data ingestion system for extracting Brazilian legislative data from the Câmara dos Deputados (Federal Chamber) public API.

## Overview

This project provides a comprehensive framework for collecting, processing, and extracting data related to:
- **Deputados** (Representatives) - Personal information, expenses, speeches, events, etc.
- **Legislatures** - Legislative sessions and periods
- **Blocos** (Political Blocs) - Political party alliances and their members
- **Frentes** (Parliamentary Fronts) - Thematic coalitions and their members
- **Partidos** (Political Parties) - Party information, leaders, and members
- **Órgãos** (Organs/Committees) - Legislative committees and their activities
- **Proposições** (Propositions) - Bills, laws, and their amendments
- **Votações** (Votes) - Voting records and individual votes

## Features

- **Asynchronous Processing**: Leverages Python's `asyncio` for efficient concurrent API calls
- **Rate Limiting**: Built-in semaphore to respect API rate limits (5 concurrent requests)
- **Retry Logic**: Exponential backoff retry strategy with configurable attempts
- **Pagination Support**: Automatic pagination handling for large datasets
- **Structured Data**: Organized extractor classes for each data entity
- **Error Handling**: Comprehensive error handling and logging

## Installation

### Prerequisites
- Python 3.8+
- pip (Python package manager)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd camara-senado-data-ingestion
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Dependencies

- **aiohttp** (≥3.8.0) - Async HTTP client for API requests
- **requests** (≥2.28.0) - Synchronous HTTP client for fallback requests
- **tenacity** (≥8.2.0) - Retry library with backoff strategies

## Project Structure

```
src/
├── main.py                          # Entry point
├── clients/
│   └── camara_client.py            # Async HTTP client with retry logic
├── extractors/
│   └── camara/
│       ├── base.py                 # Base extractor class
│       ├── blocos/                 # Political bloc extractors
│       ├── deputados/              # Representative extractors
│       ├── frentes/                # Parliamentary front extractors
│       ├── legislaturas/           # Legislature extractors
│       ├── orgaos/                 # Committee extractors
│       ├── partidos/               # Political party extractors
│       ├── proposicoes/            # Proposition extractors
│       └── votacoes/               # Vote extractors
└── utils/
    └── utils.py                    # Utility functions
```

## Usage

### Basic Example

```python
from clients.camara_client import AsyncCamaraClient
from extractors.camara.frentes.frentes import AsyncFrentesExtractor
from extractors.camara.frentes.membros import AsyncFrentesMembrosExtractor
import asyncio

async def main():
    client = AsyncCamaraClient()
    
    # Extract frentes (parliamentary fronts)
    frentes_extractor = AsyncFrentesExtractor(client)
    frentes_data = await frentes_extractor.extract(init_legislatura=56)
    
    # Extract membros (members) for each frente
    membros_extractor = AsyncFrentesMembrosExtractor(client)
    frentes_membros_data = await membros_extractor.extract(frentes_data)
    
    print(f'Total members extracted: {len(frentes_membros_data)}')
    return frentes_membros_data

if __name__ == '__main__':
    result = asyncio.run(main())
```

### Extracting Deputados (Representatives)

```python
from extractors.camara.deputados.deputados import DeputadosExtractor

extractor = DeputadosExtractor(client)
deputados = extractor.extract(
    init_legislatura=56,
    sigla_partido='PT',  # Optional: filter by party
    sigla_uf='SP',       # Optional: filter by state
    items=100            # Items per page
)
```

### Extracting Proposições (Bills)

```python
from extractors.camara.proposicoes.proposicoes import ProposicoesExtractor

extractor = ProposicoesExtractor(client)
proposicoes = extractor.extract(
    init_legislatura=56,
    itens=100
)
```

## Extractor Modules

### Async Extractors
These use `asyncio` for concurrent requests:
- `AsyncFrentesExtractor` - Parliamentary fronts
- `AsyncFrentesMembrosExtractor` - Front members
- `AsyncFrentesIdsExtractor` - Front detailed info
- `AsyncBlocosExtractor` - Political blocs
- `AsyncBlocosIdsExtractor` - Bloc detailed info
- `AsyncBlocosPartidosExtractor` - Bloc parties
- `AsyncLegislaturaExtractor` - Legislatures
- `AsyncPartidosExtractor` - Political parties
- `AsyncPartidosMembrosExtractor` - Party members
- `AsyncOrgaosExtractor` - Committees
- `AsyncEventosExtractor` - Committee events
- `AsyncMembrosExtractor` - Committee members
- `AsyncOrgaosVotacoesExtractor` - Committee votes
- `AsyncVotacoesExtractor` - Voting records
- `AsyncVotacoesOrientacoes` - Voting orientations
- `AsyncVotosExtractor` - Individual votes

### Synchronous Extractors
These use traditional synchronous requests:
- `DeputadosExtractor` - Representative information
- `IdsExtractor` (deputados) - Detailed representative data
- `DespesasExtractor` - Representative expenses
- `DiscursosExtractor` - Speeches
- `EventosExtractor` - Events
- `FrentesExtractor` - Front memberships
- `HistoricoExtractor` - Historical data
- `OcupacoesExtractor` - Occupations
- `ProposicoesExtractor` - Bills and propositions
- `VotacoesExtractor` (proposicoes) - Proposition votes

### Reference Extractors
These fetch static reference data:
- `CodigoSituacaoExtractor` - Status codes
- `CodigoTemaExtractor` - Topic codes
- `CodigoTipoAutorExtractor` - Author type codes
- `CodigoTipoTramitacaoExtractor` - Processing type codes
- `SiglaTipoExtractor` - Type abbreviations
- `SituacoesProposicaoExtractor` - Proposition statuses
- `TiposAutorExtractor` - Author types
- `TiposProposicaoExtractor` - Proposition types
- `TiposTramitacaoExtractor` - Processing types
- `SituacoesOrgaoExtractor` - Committee statuses
- `CodigoSituacaoOrgaoExtractor` - Committee status codes

## API Client Configuration

The `AsyncCamaraClient` can be customized:

```python
from clients.camara_client import AsyncCamaraClient

# Default: https://dadosabertos.camara.leg.br/api/v2/
client = AsyncCamaraClient()

# Or with custom URL
client = AsyncCamaraClient(url='https://custom-api.example.com/')
```

## Rate Limiting

The client uses a semaphore to limit concurrent requests to 5 at a time. This can be adjusted:

```python
client = AsyncCamaraClient()
client._semaphore = asyncio.Semaphore(10)  # Increase to 10 concurrent requests
```

## Error Handling

The client includes automatic retry logic:
- Maximum 5 attempts per request
- Exponential backoff (2-20 seconds)
- Automatic recovery on HTTP errors

See [camara_client.py](src/clients/camara_client.py) for detailed implementation.

## Data Structure

All extractors return data in one of two formats:

1. **List of dictionaries** containing:
   - Original API response fields
   - Added foreign key fields (e.g., `idLegislatura`, `idFrente`)

2. **Single dictionary** for detailed entity information

Most extractors automatically add relation IDs to facilitate data integration.

## Utilities

### Date Manipulation

```python
from utils.utils import add_months
from datetime import date

new_date = add_months(date(2023, 1, 15), 3)  # Returns: 2023-04-01
```

## Best Practices

1. **Use async extractors for better performance** - Especially when fetching large datasets
2. **Handle pagination** - Set appropriate `request_tries` and `itens` parameters
3. **Filter by legislature** - Use `init_legislatura` to limit scope
4. **Monitor API responses** - Check logged messages for any issues
5. **Implement caching** - Store results to avoid redundant API calls

## Troubleshooting

### Connection Timeouts
- Increase timeout value in `AsyncCamaraClient.get()` method (default: 20 seconds)
- Reduce concurrent request limit via semaphore

### Rate Limiting (429 responses)
- The client automatically handles 429 responses with Retry-After header
- Reduce concurrent requests or increase delays between batches

### Empty Responses
- Some endpoints may have pagination limits
- Check the returned `empty_count` threshold in extractors

## API Documentation

For detailed information about the Câmara dos Deputados API, visit:
https://dadosabertos.camara.leg.br/

## Contributing

When adding new extractors:

1. Inherit from `CamaraBaseExtractor`
2. Define the appropriate `ENDPOINT`
3. Implement the `extract()` method
4. Follow existing naming conventions
5. Add proper error handling and logging
6. Update this README with new extractor documentation

## License

[Add your license information here]

## Author

[Add author information here]

## Support

For issues and questions, please [add support contact information]
