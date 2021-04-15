"""Stream class for tap-procore."""


import requests

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable


from singer_sdk.streams import RESTStream



from singer_sdk.authenticators import (
    APIAuthenticatorBase,
    SimpleAuthenticator,
    OAuthAuthenticator,
    OAuthJWTAuthenticator
)

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

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class ProcoreAuthenticator(OAuthAuthenticator):

    @property
    def oauth_request_body(self) -> dict:
        req = {
            'grant_type': 'refresh_token',
            'client_id': self.config["client_id"],
            'client_secret': self.config["client_secret"],
            'refresh_token': self.config["refresh_token"],
            'redirect_uri': self.config["redirect_uri"]
        }

        return req


class ProcoreStream(RESTStream):
    """Procore stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://sandbox.procore.com/rest/v1.0" if self.config["is_sandbox"] else "https://api.procore.com/rest/v1.0"

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        auth_endpoint = "https://login-sandbox.procore.com/oauth/token" if self.config["is_sandbox"] else "https://login.procore.com/oauth/token"
        print(auth_endpoint)

        return ProcoreAuthenticator(
            stream=self,
            auth_endpoint=auth_endpoint
        )


class CompaniesStream(ProcoreStream):
    name = "companies"

    path = "/companies"

    primary_keys = ["id"]
    replication_key = None

    schema = PropertiesList(
        Property("id", IntegerType),
        Property("is_active", BooleanType),
        Property("name", StringType)
    ).to_dict()


# class ProjectsStream(ProcoreStream):
#     name = "projects"

#     path = "/projects"

#     primary_keys = ["id"]
#     replication_key = "updated_at"

#     schema = PropertiesList(
#         Property("id", IntegerType),
#         Property("name", StringType),
#         Property("display_name", StringType),
#         Property("project_number", StringType),
#         Property("address", StringType),
#         Property("city", StringType),
#         Property("state_code", StringType),
#         Property("country_code", StringType),
#         Property("zip", StringType),
#         Property("county", StringType),
#         Property("time_zone", StringType),
#         Property("latitude", StringType),
#         Property("longitude", StringType),
#         Property("stage", StringType),
#         Property("phone", StringType),
#         Property("created_at", DateTimeType),
#         Property("updated_at", DateTimeType),
#         Property("active", BooleanType),
#         Property("company", ObjectType),
#     ).to_dict()
