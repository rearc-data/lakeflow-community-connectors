"""
sources/wiz/wiz_client.py
-----------------------
The Wiz API client.

This is your WizGraphQLClient copied verbatim from WIZ_Collector notebook
Step 2. Zero logic changes — just moved into its own file.

INTERFACE CONTRACT
------------------
source.py calls these two methods on self._client:

    client.execute(query, variables)         → dict
    client.paginate_connection(
        query=, connection_field=,
        variables=, page_size=
    )                                        → list[dict]

WizMockClient in api_mock.py implements the same two methods
with fake data. source.py never imports from this file directly
— it receives the client through __init__ options.
"""
import json
import time
from dataclasses import dataclass, field
from typing import Any
from urllib import error, parse, request as urllib_request


class GraphQLClientError(RuntimeError):
    """Raised when OAuth or GraphQL requests fail."""


@dataclass(slots=True)
class WizGraphQLClient:
    """
    Production GraphQL client for the Wiz API.

    Handles OAuth2 client-credentials token fetch + auto-refresh,
    single GraphQL execute(), and full cursor-paginated paginate_connection().

    SOURCE: copied verbatim from WIZ_Collector notebook Step 2.
    """

    base_url:        str
    client_id:       str | None = None
    client_secret:   str | None = None
    audience:        str        = "wiz-api"
    timeout_seconds: int        = 60
    auth_enabled:    bool       = True
    access_token:    str | None = None
    _token_expiry:   float      = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        if self.auth_enabled and not self.access_token:
            if not self.client_id or not self.client_secret:
                raise GraphQLClientError(
                    "client_id and client_secret required when auth_enabled=True"
                )

    @property
    def oauth_url(self) -> str:
        return f"{self.base_url}/oauth/token"

    @property
    def graphql_url(self) -> str:
        return f"{self.base_url}/graphql"

    def authenticate(self) -> str | None:
        """Fetch and cache bearer token; auto-refreshes before expiry."""
        if not self.auth_enabled:
            return None
        if self.access_token and time.time() < self._token_expiry - 60:
            return self.access_token
        payload = parse.urlencode({
            "grant_type":    "client_credentials",
            "audience":      self.audience,
            "client_id":     self.client_id,
            "client_secret": self.client_secret,
        }).encode("utf-8")
        resp = self._send_request(
            url=self.oauth_url, method="POST", body=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token = resp.get("access_token")
        if not isinstance(token, str) or not token:
            raise GraphQLClientError("OAuth response missing access_token")
        self.access_token  = token
        self._token_expiry = time.time() + int(resp.get("expires_in", 3300))
        return token

    def execute(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute one GraphQL operation and return the data dict."""
        token   = self.authenticate()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = self._send_request(
            url=self.graphql_url, method="POST",
            body=json.dumps({"query": query, "variables": variables or {}}).encode(),
            headers=headers,
        )
        errors = resp.get("errors")
        if isinstance(errors, list) and errors:
            raise GraphQLClientError(f"GraphQL errors: {json.dumps(errors)}")
        data = resp.get("data")
        if not isinstance(data, dict):
            raise GraphQLClientError("GraphQL response missing 'data' object")
        return data

    def paginate_connection(
        self,
        *,
        query: str,
        connection_field: str,
        variables: dict[str, Any] | None = None,
        page_size: int = 500,
        max_pages: int | None = None,
        on_page: Any = None,
    ) -> list[dict[str, Any]]:
        """
        Collect all nodes from a Wiz-style cursor-paginated connection.

        on_page: optional callback(batch, page_num, end_cursor)
        """
        vars_: dict[str, Any] = dict(variables or {})
        vars_["first"] = page_size
        if "after" not in vars_:
            vars_["after"] = None

        nodes: list[dict[str, Any]] = []
        page_count = 0

        while True:
            data       = self.execute(query=query, variables=vars_)
            connection = data.get(connection_field)
            if not isinstance(connection, dict):
                raise GraphQLClientError(f"Missing connection field '{connection_field}'")

            batch      = [n for n in connection.get("nodes", []) if isinstance(n, dict)]
            nodes.extend(batch)

            page_info  = connection.get("pageInfo", {})
            has_next   = bool(page_info.get("hasNextPage"))
            end_cursor = page_info.get("endCursor")
            page_count += 1

            if callable(on_page):
                on_page(batch, page_count, end_cursor)

            if not has_next:
                break
            if max_pages is not None and page_count >= max_pages:
                break
            if not isinstance(end_cursor, str) or not end_cursor:
                raise GraphQLClientError("hasNextPage=true but endCursor is empty")
            vars_["after"] = end_cursor

        return nodes

    def _send_request(
        self,
        *,
        url: str,
        method: str,
        body: bytes,
        headers: dict[str, str],
    ) -> dict[str, Any]:
        req = urllib_request.Request(url=url, data=body, method=method, headers=headers)
        try:
            with urllib_request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            raise GraphQLClientError(
                f"HTTP {exc.code} from {url}: {exc.read().decode('utf-8', errors='replace')}"
            ) from exc
        except error.URLError as exc:
            raise GraphQLClientError(f"Request failed for {url}: {exc.reason}") from exc
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise GraphQLClientError(f"Non-JSON from {url}: {raw[:200]}") from exc
        if not isinstance(parsed, dict):
            raise GraphQLClientError(f"Response from {url} was not a JSON object")
        return parsed


def get_wiz_client(options: dict) -> WizGraphQLClient:
    """
    Factory function — mirrors get_api() pattern from the example connector.

    Called by WizLakeflowConnect.__init__ when options["use_mock"] is absent
    or False. Reads credentials from the options dict which comes from
    spec.yaml → Databricks secret scope.

    options dict expected keys (same format as your existing collector secret):
        base_url       : "https://api.us20.app.wiz.io"
        client_id      : "..."
        client_secret  : "..."
        audience       : "wiz-api"           (optional, default shown)
        auth_enabled   : true                (optional, default true)
    """
    return WizGraphQLClient(
        base_url      = options["base_url"],
        client_id     = options.get("client_id"),
        client_secret = options.get("client_secret"),
        audience      = options.get("audience", "wiz-api"),
        auth_enabled  = options.get("auth_enabled", True),
    )