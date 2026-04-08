#from __future__ import annotations
import json
import time
from dataclasses import dataclass, field
from typing import Any
from urllib import error, parse, request as urllib_request


class GraphQLClientError(RuntimeError):
    """Raised when OAuth or GraphQL requests fail."""


@dataclass(slots=True)
class WizGraphQLClientMock:
    """Production GraphQL client for the Wiz API."""

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
                raise GraphQLClientError("client_id and client_secret required when auth_enabled=True")

    @property
    def oauth_url(self)    -> str: return f"{self.base_url}/oauth/token"
    @property
    def graphql_url(self)  -> str: return f"{self.base_url}/graphql"

    def authenticate(self) -> str | None:
        """Fetch and cache bearer token; auto-refreshes before expiry."""
        if not self.auth_enabled:
            return None
        if self.access_token and time.time() < self._token_expiry - 60:
            return self.access_token
        payload = parse.urlencode({
            "grant_type": "client_credentials", "audience": self.audience,
            "client_id": self.client_id, "client_secret": self.client_secret,
        }).encode("utf-8")
        resp = self._send_request(url=self.oauth_url, method="POST", body=payload,
                                  headers={"Content-Type": "application/x-www-form-urlencoded"})
        token = resp.get("access_token")
        if not isinstance(token, str) or not token:
            raise GraphQLClientError("OAuth response missing access_token")
        self.access_token = token
        self._token_expiry = time.time() + int(resp.get("expires_in", 3300))
        return token

    def execute(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute one GraphQL operation and return the data dict."""
        token   = self.authenticate()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp   = self._send_request(url=self.graphql_url, method="POST",
                                     body=json.dumps({"query": query, "variables": variables or {}}).encode(),
                                     headers=headers)
        errors = resp.get("errors")
        if isinstance(errors, list) and errors:
            raise GraphQLClientError(f"GraphQL errors: {json.dumps(errors)}")
        data = resp.get("data")
        if not isinstance(data, dict):
            raise GraphQLClientError("GraphQL response missing 'data' object")
        return data

    def paginate_connection(self, *, query: str, connection_field: str,
                             variables: dict[str, Any] | None = None,
                             page_size: int = 500, max_pages: int | None = None,
                             on_page: Any = None) -> list[dict[str, Any]]:
        """
        Collect all nodes from a Wiz-style cursor-paginated connection.

        on_page: optional callback(batch, page_num, end_cursor) — used by
                 detection collector to save cursor after every page.
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

            batch = [n for n in connection.get("nodes", []) if isinstance(n, dict)]
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
                print(f"    ⚠️  max_pages={max_pages} reached")
                break
            if not isinstance(end_cursor, str) or not end_cursor:
                raise GraphQLClientError("hasNextPage=true but endCursor is empty")
            vars_["after"] = end_cursor

        return nodes

    def _send_request(self, *, url: str, method: str, body: bytes,
                       headers: dict[str, str]) -> dict[str, Any]:
        req = urllib_request.Request(url=url, data=body, method=method, headers=headers)
        try:
            with urllib_request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            raise GraphQLClientError(f"HTTP {exc.code} from {url}: {exc.read().decode('utf-8', errors='replace')}") from exc
        except error.URLError as exc:
            raise GraphQLClientError(f"Request failed for {url}: {exc.reason}") from exc
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise GraphQLClientError(f"Non-JSON from {url}: {raw[:200]}") from exc
        if not isinstance(parsed, dict):
            raise GraphQLClientError(f"Response from {url} was not a JSON object")
        return parsed


def get_mock_wiz_client(options: dict) -> WizGraphQLClientMock:
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
    print("Options are:", options)
    # 🔥 handle JSON secret
    if "config_json" in options:
        config = json.loads(options["config_json"])
    else:
        config = options

    #config = options
    return WizGraphQLClientMock(
        base_url=config["base_url"],
        client_id=config.get("client_id"),
        client_secret=config.get("client_secret"),
        audience=config.get("audience", "wiz-api"),
        auth_enabled=config.get("auth_enabled", True)
    )