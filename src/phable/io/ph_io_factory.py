from phable.io.json_decoder import JsonDecoder
from phable.io.json_encoder import JsonEncoder
from phable.io.parquet_io import ParquetDecoder, ParquetEncoder
from phable.io.zinc_decoder import ZincDecoder
from phable.io.zinc_encoder import ZincEncoder

PH_IO_FACTORY = {
    "zinc": {
        "content_type": "text/zinc",
        "encoder": ZincEncoder(),
        "decoder": ZincDecoder(),
    },
    "json": {
        "content_type": "application/json",
        "encoder": JsonEncoder(),
        "decoder": JsonDecoder(),
    },
    "parquet": {
        "content_type": "application/octet-stream",
        "encoder": ParquetEncoder(),
        "decoder": ParquetDecoder(),
    },
}
