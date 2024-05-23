from dagster import ConfigurableResource
from sodapy import Socrata


class SocrataClientResource(ConfigurableResource):
    url: str
    app_token: str

    def getClient(self) -> Socrata:
        return Socrata(self.url, self.app_token)
