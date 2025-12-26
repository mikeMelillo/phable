import pytest

from typing import Generator

from phable.io.zinc_decoder import ZincDecoder
from phable.haxall_client import HaxallClient

ZINC_SOURCE = "./tests/resources/haystack_alpha.zinc"

# For Haxall config
# Eventually move these to a user .env file with def vals here
URI = "http://localhost:8080/api/sys"
USERNAME = "su"
PASSWORD = "su"

@pytest.fixture(params=["json", "zinc"], scope="module")
def client(request) -> Generator[HaxallClient, None, None]:
    # use HxClient's features to test Client
    hc = HaxallClient.open(URI, USERNAME, PASSWORD, content_type=request.param)
    yield hc
    hc.close()

@pytest.mark.order(0)
def test_import_demo_proj(client: HaxallClient):
    print(client.about())   
    with open(ZINC_SOURCE, "r") as zinc:
        zinc_data = ZincDecoder().from_str(zinc.read())
    print(zinc_data.meta)
    print(zinc_data.cols)

