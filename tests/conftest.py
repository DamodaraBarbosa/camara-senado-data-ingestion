import pytest
from unittest.mock import AsyncMock

@pytest.fixture
def mock_client():
    """Fixture that provides an AsyncMock for the Camara client."""
    client = AsyncMock()
    # Default behavior for get can be set here if needed
    return client
