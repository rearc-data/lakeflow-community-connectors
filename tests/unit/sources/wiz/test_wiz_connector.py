from databricks.labs.community_connector.sources.wiz.wiz_mock_api import reset_mock_api
from databricks.labs.community_connector.sources.wiz.wiz import WizLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestWizConnector(LakeflowConnectTests):
    connector_class = WizLakeflowConnect
    sample_records = 50

    @classmethod
    def setup_class(cls):
        cls.config = cls._load_config()
        reset_mock_api()
        super().setup_class()
