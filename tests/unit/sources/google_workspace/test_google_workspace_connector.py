from databricks.labs.community_connector.sources.google_workspace.google_workspace_mock_api import reset_mock_api
from databricks.labs.community_connector.sources.google_workspace.google_workspace import GoogleWorkspaceLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGoogleWorkspaceConnector(LakeflowConnectTests):
    connector_class = GoogleWorkspaceLakeflowConnect
    sample_records = 50

    @classmethod
    def setup_class(cls):
        cls.config = cls._load_config()
        reset_mock_api()
        super().setup_class()
