"""Stream class for tap-procore."""


import requests

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast

from singer.schema import Schema

from singer_sdk.streams import RESTStream
from singer_sdk.helpers._util import utc_now
from singer_sdk.plugin_base import PluginBase as TapBaseClass


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
            'refresh_token': self.config["refresh_token"] if not self.refresh_token else self.refresh_token,
            'redirect_uri': self.config["redirect_uri"]
        }

        return req

    def update_access_token(self):
        """Update `access_token` along with: `last_refreshed` and `expires_in`."""
        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload
        token_response = requests.post(self.auth_endpoint, data=auth_request_payload)
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json["expires_in"]
        self.last_refreshed = request_time

        if token_json.get("refresh_token") is not None:
            self.refresh_token = token_json["refresh_token"]


class ProcoreStream(RESTStream):
    """Procore stream class."""

    def __init__(
        self,
        tap: TapBaseClass,
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, Any], Schema]] = None,
        path: Optional[str] = None,
    ):
        """Initialize the Procore stream."""
        super().__init__(name=name, schema=schema, tap=tap, path=path)
        self._config = tap._config

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://sandbox.procore.com/rest/v1.0" if self.config["is_sandbox"] else "https://api.procore.com/rest/v1.0"

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        if not self._config.get("authenticator"):
            auth_endpoint = "https://login-sandbox.procore.com/oauth/token" if self.config[
                "is_sandbox"] else "https://login.procore.com/oauth/token"

            self._config["authenticator"] = ProcoreAuthenticator(
                stream=self,
                auth_endpoint=auth_endpoint
            )

        return self._config["authenticator"]


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

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        """Prepare a request object.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.
        """
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers(context)

        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data,
                )
            ),
        )
        return request

    def http_headers(self, context: Optional[dict] = None) -> dict:
        """Return headers dict to be used for HTTP requests.

        If an authenticator is also specified, the authenticator's headers will be
        combined with `http_headers` when making HTTP requests.
        """
        result = super().http_headers
        result['Procore-Company-Id'] = str(context['company_id'])
        return result

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # Add project_id to response
        row['company_id'] = context['company_id']
        return row

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
        Property("company_id", IntegerType),
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
            headers['Procore-Company-Id'] = str(company['id'])
            r = requests.get(endpoint, headers=headers)
            def add_company(x):
                x['company_id'] = company['id']
                return x
            l = list(map(add_company, r.json()))
            projects.extend(l)

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
                'project_id': project['id'],
                'company_id': project['company_id']
            })
        return result or None

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # Add project_id to response
        row['project_id'] = context['project_id']
        return row

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
        Property("project_id", IntegerType),
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
            headers['Procore-Company-Id'] = str(company['id'])
            r = requests.get(endpoint, headers=headers)
            def add_company(x):
                x['company_id'] = company['id']
                return x
            l = list(map(add_company, r.json()))
            projects.extend(l)

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
                'project_id': project['id'],
                'company_id': project['company_id']
            })
        return result or None

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # Add project_id to response
        row['project_id'] = context['project_id']
        return row

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
        Property("project_id", IntegerType),
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
            headers['Procore-Company-Id'] = str(company['id'])
            r = requests.get(endpoint, headers=headers)
            def add_company(x):
                x['company_id'] = company['id']
                return x
            l = list(map(add_company, r.json()))
            projects.extend(l)

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
                'project_id': project['id'],
                'company_id': project['company_id']
            })
        return result or None

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # Add project_id to response
        row['project_id'] = context['project_id']
        return row

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
        Property("project_id", IntegerType),
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
        endpoint = f"{self.url_base}/folders/{folder}?project_id={project['id']}"
        r = requests.get(endpoint, headers=headers)
        raw_data = r.json()
        data = raw_data.get('folders', [])

        # Recursively get subfolders
        for f in data:
            self.logger.info(f"Found folder {f['name_with_path']}")
            folders.extend(self.get_subfolders(headers, f['id'], project))

        # Add these folders to final output
        folders.extend([{'folder': x['id'], 'project': project['id'], 'company_id': project['company_id']} for x in data])

        return folders

    def get_folders(self, headers):
        projects = self.get_projects(headers)
        folders = []

        for project in projects:
            endpoint = f"{self.url_base}/folders?project_id={project['id']}"
            r = requests.get(endpoint, headers=headers)
            data = r.json().get('folders', [])

            # Add root folder
            folders.append({'folder': -1, 'project': project['id'], 'company_id': project['company_id']})

            # Add these folders to final output
            folders.extend(
                [{'folder': x['id'], 'project': project['id'], 'company_id': project['company_id']} for x in data])

            # Recursively get subfolders
            for f in data:
                folders.extend(self.get_subfolders(headers, f['id'], project))

        return folders

    def get_url(self, partition: Optional[dict]) -> str:
        url = super().get_url(partition)

        # Handle sub folders
        if partition['folder'] != -1:
            sub_url = f"{url}/{partition['folder']}"
            return sub_url

        return url

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

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # Add project_id to response
        row['project_id'] = context['project']
        return row

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
        Property("project_id", IntegerType),
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


class PurchaseOrderStream(ProjectsStream):
    name = "purchase_order_contracts"

    path = "/purchase_order_contracts"

    primary_keys = ["id"]
    replication_key = None

    def get_projects(self, headers):
        companies = self.get_companies(headers)
        projects = []

        for company in companies:
            endpoint = f"{self.url_base}/projects?company_id={company['id']}"
            headers['Procore-Company-Id'] = str(company['id'])
            r = requests.get(endpoint, headers=headers)
            def add_company(x):
                x['company_id'] = company['id']
                return x
            l = list(map(add_company, r.json()))
            projects.extend(l)

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
                'project_id': project['id'],
                'company_id': project['company_id']
            })
        return result or None

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # Add project_id to response
        row['project_id'] = context['project_id']
        return row

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
        Property("accounting_method", StringType),
        Property("approval_letter_date", StringType),
        Property("approved_change_orders", StringType),
        Property("assignee", ObjectType(
            Property("id", IntegerType),
        )),
        Property("bill_to_address", StringType),
        Property("contract_date", StringType),
        Property("created_at", DateTimeType),
        Property("deleted_at", DateTimeType),
        Property("delivery_date", StringType),
        Property("description", StringType),
        Property("draft_change_orders_amount", StringType),
        Property("executed", BooleanType),
        Property("execution_date", StringType),
        Property("grand_total", StringType),
        Property("issued_on_date", StringType),
        Property("letter_of_intent_date", StringType),
        Property("number", StringType),
        Property("origin_code", StringType),
        Property("origin_data", StringType),
        Property("origin_id", IntegerType),
        Property("payment_terms", StringType),
        Property("pending_change_orders", StringType),
        Property("pending_revised_contract", StringType),
        Property("percentage_paid", IntegerType),
        Property("private", BooleanType),
        Property("project", ObjectType(
            Property("id", IntegerType),
            Property("name", StringType),
        )),
        Property("remaining_balance_outstanding", StringType),
        Property("requisitions_are_enabled", BooleanType),
        Property("retainage_percent", StringType),
        Property("returned_date", StringType),
        Property("revised_contract", StringType),
        Property("ship_to_address", StringType),
        Property("ship_via", StringType),
        Property("show_line_items_to_non_admins", BooleanType),
        Property("signed_contract_received_date", StringType),
        Property("status", StringType),
        Property("title", StringType),
        Property("total_draw_requests_amount", StringType),
        Property("total_payments", StringType),
        Property("total_requisitions_amount", IntegerType),
        Property("updated_at", DateTimeType),
        Property("vendor", ObjectType(
            Property("id", IntegerType),
            Property("company", StringType),
        ))
    ).to_dict()