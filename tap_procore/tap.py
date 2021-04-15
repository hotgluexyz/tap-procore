"""procore tap class."""

from pathlib import Path
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

# TODO: Import your custom stream types here:
from tap_procore.streams import (
    ProcoreStream,

    CompaniesStream,
)


# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    CompaniesStream
]


class TapProcore(Tap):
    """Procore tap class."""

    name = "tap-procore"

    config_jsonschema = PropertiesList(
        Property("client_id", StringType, required=True),
        Property("client_secret", StringType, required=True),
        Property("refresh_token", StringType, required=True),
        Property("redirect_uri", StringType, required=True),
        Property("is_sandbox", BooleanType, default=False),
        Property("start_date", DateTimeType)
    ).to_dict()


    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


# CLI Execution:

cli = TapProcore.cli
