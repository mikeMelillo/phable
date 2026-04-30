from phable.io.json_decoder import JsonDecoder
from phable.io.json_encoder import JsonEncoder
from phable.io.zinc_decoder import ZincDecoder
from phable.io.zinc_encoder import ZincEncoder
from phable.io.parquet_decoder import ParquetDecoder
from phable.io.parquet_encoder import ParquetEncoder

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