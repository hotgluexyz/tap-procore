"""procore tap class."""
import json
import traceback

from pathlib import Path, PurePath
from typing import List, Optional, Union

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

from tap_procore.streams import (
    ProcoreStream,
    CompaniesStream,
    ProjectsStream,
    FoldersStream,
    FilesStream,
    ProjectRolesStream,
    ProjectUsersStream,
    PurchaseOrderStream,
    ProjectTimecardStream,
    CompanyUserStream,
    ProjectUserStream,
)


# List of available streams
STREAM_TYPES = [
    CompaniesStream,
    ProjectsStream,
    FoldersStream,
    FilesStream,
    ProjectRolesStream,
    ProjectUsersStream,
    PurchaseOrderStream,
    ProjectTimecardStream,
    CompanyUserStream,
    ProjectUserStream,
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


    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        catalog: Union[PurePath, str, dict, None] = None,
        state: Union[PurePath, str, dict, None] = None,
        parse_env_config: bool = False,
    ) -> None:
        """Initialize the tap."""
        self.config_path = config
        super().__init__(config=config, catalog=catalog, state=state, parse_env_config=parse_env_config)


    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    def sync_all(self):
        """Sync all streams."""
        # Do the sync
        try:
            super().sync_all()
        except Exception as e:
            self.logger.error(e)
            traceback.print_exc()
        finally:
            # Update config if needed
            self.update_config()

    def update_config(self):
        """Update config.json with new access + refresh token."""
        self.logger.info("Updating config.")
        path = self.config_path
        auth = self._config.pop("authenticator", None)

        if auth is not None:
            if auth.refresh_token is not None:
                self._config["refresh_token"] = auth.refresh_token
            
            if auth.access_token is not None:
                self._config["access_token"] = auth.access_token

        if isinstance(path, list):
            path = path[0]

        with open(path, 'w') as f:
            json.dump(self._config, f, indent=4)

# CLI Execution:
cli = TapProcore.cli
if __name__ == '__main__':
    TapProcore.cli()
