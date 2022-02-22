import abc
import datetime
import enum
import json
from dataclasses import dataclass
from typing import Union


@enum.unique
class DCProduct(enum.Enum):
    wcivf = "WCIVF"
    wdiv = "WDIV"
    aggregator_api = "AGGREGATOR_API"
    ec_api = "EC_API"
    ynr = "YNR"
    election_leaflets = "ELECTION_LEAFLETS"


@dataclass
class BaseLogEntry(abc.ABC):
    def as_log_line(self, newline=True):
        newline_char = ""
        if newline:
            newline_char = "\n"
        json_data = json.dumps(self.__dict__, sort_keys=True, default=str)
        return f"{json_data}{newline_char}"


@dataclass
class ValidDCProductMixin:
    dc_product: DCProduct

    def __post_init__(self):
        if not isinstance(self.dc_product, DCProduct):
            raise ValueError(f"'{self.dc_product}' is not currently supported")
        self.dc_product = self.dc_product.value


@dataclass
class DummyLogEntry(BaseLogEntry, ValidDCProductMixin):
    text: str


@dataclass
class PostcodeLogEntry(BaseLogEntry, ValidDCProductMixin):
    postcode: str
    timestamp: Union[datetime.datetime, str] = ""
    api_key: str = ""

    def __post_init__(self):
        super().__post_init__()
        if not self.timestamp:
            self.datetime = datetime.datetime.now()
