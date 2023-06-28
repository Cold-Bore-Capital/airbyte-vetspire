from dataclasses import InitVar, dataclass
from typing import Any, List, Mapping, Optional

import requests
from airbyte_cdk.sources.declarative.requesters.paginators.strategies.pagination_strategy import PaginationStrategy
from airbyte_cdk.sources.declarative.requesters.paginators.strategies.page_increment import PageIncrement

@dataclass
class VetPageIncrement(PageIncrement):
    """
    Pagination strategy that returns the number of pages reads so far and returns it as the next page token

    Attributes:
        page_size (int): the number of records to request
        start_from_page (int): number of the initial page
    """
    def __post_init__(self, parameters: Mapping[str, Any]):
        self._offset = 0

    @abstractmethod
    def next_page_token(self, response: requests.Response, last_records: List[Mapping[str, Any]]) -> Optional[Any]:
        if len(last_records) == self.limit:
            self._offset += self.limit
            return {'offset': self._offset}
        else:
            return None

    def reset(self):
        self._offset = 0

    def get_page_size(self) -> Optional[int]:
        return self.limit
