from .api_client import ElectricityMapsClient
from .spark_session import get_spark
from .watermark import get_last_ingested, set_last_ingested

__all__ = [
    "ElectricityMapsClient",
    "get_spark",
    "get_last_ingested",
    "set_last_ingested",
]
