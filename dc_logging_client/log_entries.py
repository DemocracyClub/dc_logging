import abc
import datetime
import enum
import json
from dataclasses import dataclass, field
from typing import Union


@enum.unique
class DCProduct(enum.Enum):
    wcivf = "WCIVF"
    wdiv = "WDIV"
    aggregator_api = "AGGREGATOR_API"
    ec_api = "EC_API"
    ynr = "YNR"
    election_leaflets = "ELECTION_LEAFLETS"

    @classmethod
    def from_str_value(cls, value):
        for enum_key, enum_value in cls.__members__.items():
            if value == getattr(cls, enum_key).value:
                return getattr(cls, enum_key)
        raise ValueError("No item with string value found")


@dataclass
class BaseLogEntry(abc.ABC):
    def as_log_line(self, newline=True):
        newline_char = ""
        if newline:
            newline_char = "\n"

        data = self.__dict__
        for k, v in data.items():
            if isinstance(v, datetime.datetime):
                # Athena uses Java's SimpleDateFormat, which doesn't support
                # microseconds. We therefore need to remove the last 3 digits
                #
                # https://docs.aws.amazon.com/athena/latest/ug/data-types.html#data-types-timestamps
                data[k] = v.strftime("%Y-%m-%d %H:%M:%S.%f")[0:-3]

        json_data = json.dumps(data, sort_keys=True, default=str)
        return f"{json_data}{newline_char}"


@dataclass
class ValidDCProductMixin:
    dc_product: DCProduct

    def __post_init__(self):
        if not isinstance(self.dc_product, DCProduct):
            try:
                self.dc_product = DCProduct.from_str_value(self.dc_product)
            except ValueError:
                raise ValueError(
                    f"'{self.dc_product}' is not currently supported"
                )
        self.dc_product = self.dc_product.value


@dataclass
class UTMMixin:
    utm_source: Union[None, str] = field(default_factory=str)
    utm_campaign: Union[None, str] = field(default_factory=str)
    utm_medium: Union[None, str] = field(default_factory=str)


@dataclass
class PostcodeLogEntry(BaseLogEntry, UTMMixin, ValidDCProductMixin):
    postcode: str = field(default_factory=str)
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.now)
    api_key: str = ""
    calls_devs_dc_api: bool = False

    def __post_init__(self):
        super().__post_init__()

        if not self.postcode:
            raise ValueError("Postcode required")
        if not self.timestamp:
            self.timestamp = datetime.datetime.now()

        # Handle deprecated functionality of passing in a string timestamp
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.datetime.fromisoformat(self.timestamp)
