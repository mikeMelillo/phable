import pytest

from phable.io.zinc_decoder import ZincDecoder

ZINC_SOURCE = "./tests/resources/haystack_alpha.zinc"

@pytest.mark.order(0)
def test_import_demo_proj():
    with open(ZINC_SOURCE, "r") as zinc:
        zinc_data = ZincDecoder().from_str(zinc.read())
    print(zinc_data.meta)
    print(zinc_data.cols)

