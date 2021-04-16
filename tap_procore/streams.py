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
        auth_endpoint = "https://login-sandbox.procore.com/oauth/token" if self.config[
            "is_sandbox"] else "https://login.procore.com/oauth/token"

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


class ProjectsStream(ProcoreStream):
    name = "projects"

    path = "/projects"

    primary_keys = ["id"]
    replication_key = None

    def get_companies(self, headers):
        endpoint = f"{self.url_base}/companies"
        r = requests.get(endpoint, headers=headers)
        companies = r.json()
        return companies

    @property
    def partitions(self) -> Optional[List[dict]]:
        """Return a list of partition key dicts (if applicable), otherwise None.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.
        Developers may override this property to provide a default partitions list.
        """
        result: List[dict] = []
        headers = self.authenticator.auth_headers
        companies = self.get_companies(headers)

        for company in companies:
            result.append({
                'company_id': company['id']
            })
        return result or None

    def get_url_params(
        self,
        partition: Optional[dict],
        next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        If paging is supported, developers may override this method with specific paging
        logic.
        """
        params = {}
        params["company_id"] = partition["company_id"]
        return params

    schema = PropertiesList(
        Property("id", IntegerType),
        Property("name", StringType)
    ).to_dict()


class FoldersStream(ProjectsStream):
    name = "folders"

    path = "/folders"

    primary_keys = ["id"]
    replication_key = None

    def get_projects(self, headers):
        companies = self.get_companies(headers)
        projects = []

        for company in companies:
            endpoint = f"{self.url_base}/projects?company_id={company['id']}"
            r = requests.get(endpoint, headers=headers)
            projects.extend(r.json())

        return projects

    @property
    def partitions(self) -> Optional[List[dict]]:
        """Return a list of partition key dicts (if applicable), otherwise None.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.
        Developers may override this property to provide a default partitions list.
        """
        result: List[dict] = []
        headers = self.authenticator.auth_headers
        projects = self.get_projects(headers)

        for project in projects:
            result.append({
                'project_id': project['id']
            })
        return result or None

    def get_url_params(
        self,
        partition: Optional[dict],
        next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        If paging is supported, developers may override this method with specific paging
        logic.
        """
        params = {}
        params["project_id"] = partition["project_id"]
        return params

    schema = PropertiesList(
        Property("id", IntegerType),
        Property("name", StringType)
    ).to_dict()


class ProjectRolesStream(ProjectsStream):
    name = "project_roles"

    path = "/project_roles"

    primary_keys = ["id"]
    replication_key = None

    def get_projects(self, headers):
        companies = self.get_companies(headers)
        projects = []

        for company in companies:
            endpoint = f"{self.url_base}/projects?company_id={company['id']}"
            r = requests.get(endpoint, headers=headers)
            projects.extend(r.json())

        return projects

    @property
    def partitions(self) -> Optional[List[dict]]:
        """Return a list of partition key dicts (if applicable), otherwise None.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.
        Developers may override this property to provide a default partitions list.
        """
        result: List[dict] = []
        headers = self.authenticator.auth_headers
        projects = self.get_projects(headers)

        for project in projects:
            result.append({
                'project_id': project['id']
            })
        return result or None

    def get_url_params(
        self,
        partition: Optional[dict],
        next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        If paging is supported, developers may override this method with specific paging
        logic.
        """
        params = {}
        params["project_id"] = partition["project_id"]
        return params

    schema = PropertiesList(
        Property("id", IntegerType),
        Property("name", StringType),
        Property("role", StringType),
        Property("user_id", IntegerType),
        Property("is_active", BooleanType)
    ).to_dict()

class ProjectUsersStream(ProjectsStream):
    name = "users"

    path = "/projects"

    primary_keys = ["id"]
    replication_key = None

    def get_projects(self, headers):
        companies = self.get_companies(headers)
        projects = []

        for company in companies:
            endpoint = f"{self.url_base}/projects?company_id={company['id']}"
            r = requests.get(endpoint, headers=headers)
            projects.extend(r.json())

        return projects

    def get_url(self, partition: Optional[dict]) -> str:
        url = super().get_url(partition)
        sub_url = f"{url}/{partition['project_id']}/users"
        return sub_url

    @property
    def partitions(self) -> Optional[List[dict]]:
        """Return a list of partition key dicts (if applicable), otherwise None.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.
        Developers may override this property to provide a default partitions list.
        """
        result: List[dict] = []
        headers = self.authenticator.auth_headers
        projects = self.get_projects(headers)

        for project in projects:
            result.append({
                'project_id': project['id']
            })
        return result or None

    def get_url_params(
        self,
        partition: Optional[dict],
        next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        If paging is supported, developers may override this method with specific paging
        logic.
        """
        return {}

    schema = PropertiesList(
        Property("id", IntegerType),
        Property("name", StringType),
        Property("first_name", StringType),
        Property("last_name", StringType),
        Property("address", StringType),
        Property("email_address", StringType),
        Property("mobile_phone", StringType),
        Property("vendor", ObjectType(
            Property("id", IntegerType),
            Property("name", StringType)
        ))
    ).to_dict()

class FilesStream(FoldersStream):
    name = "files"

    path = "/folders"

    primary_keys = ["id"]
    replication_key = None

    def get_subfolders(self, headers, folder, project):
        folders = []
        endpoint = f"{self.url_base}/folders/{folder}?project_id={project}"
        r = requests.get(endpoint, headers=headers)
        raw_data = r.json()
        data = raw_data.get('folders', [])

        # Recursively get subfolders
        for f in data:
            self.logger.info(f"Found folder {f['name_with_path']}")
            folders.extend(self.get_subfolders(headers, f['id'], project))

        # Add these folders to final output
        folders.extend([{'folder': x['id'], 'project': project} for x in data])

        return folders

    def get_folders(self, headers):
        projects = self.get_projects(headers)
        folders = []

        for project in projects:
            endpoint = f"{self.url_base}/folders?project_id={project['id']}"
            r = requests.get(endpoint, headers=headers)
            data = r.json().get('folders', [])

            # Add these folders to final output
            folders.extend(
                [{'folder': x['id'], 'project': project['id']} for x in data])

            # Recursively get subfolders
            for f in data:
                folders.extend(self.get_subfolders(headers, f['id'], project['id']))

        return folders

    def get_url(self, partition: Optional[dict]) -> str:
        url = super().get_url(partition)
        sub_url = f"{url}/{partition['folder']}"
        return sub_url

    @property
    def partitions(self) -> Optional[List[dict]]:
        """Return a list of partition key dicts (if applicable), otherwise None.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.
        Developers may override this property to provide a default partitions list.
        """
        headers = self.authenticator.auth_headers
        folders = self.get_folders(headers)
        return folders

    def get_url_params(
        self,
        partition: Optional[dict],
        next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        If paging is supported, developers may override this method with specific paging
        logic.
        """
        params = {}
        params["project_id"] = partition["project"]
        return params

    schema = PropertiesList(
        Property("id", IntegerType),
        Property("name", StringType),
        Property("name_with_path", StringType),
        Property("files", ArrayType(ObjectType(
            Property("id", IntegerType),
            Property("name", StringType),
            Property("name_with_path", StringType),
            Property("file_versions", ArrayType(ObjectType(
                Property("id", IntegerType),
                Property("file_id", IntegerType),
                Property("url", StringType),
                Property("created_at", DateTimeType),
                Property("prostore_file", ObjectType(
                    Property("id", IntegerType),
                    Property("name", StringType),
                    Property("url", StringType),
                    Property("filename", StringType)
                ))
            )))
        )))
    ).to_dict()
