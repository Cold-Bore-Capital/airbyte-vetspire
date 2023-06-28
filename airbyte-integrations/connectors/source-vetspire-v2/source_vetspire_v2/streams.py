from abc import ABC
from dataclasses import dataclass

from airbyte_cdk.sources.declarative.schema.json_file_schema_loader import JsonFileSchemaLoader
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
import requests
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import SimpleRetriever
from urllib.parse import parse_qsl, urlparse

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
# @dataclass
class VetspireV2Stream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class VetspireV2Stream(HttpStream, ABC)` which is the current class
    `class Customers(VetspireV2Stream)` contains behavior to pull data for customers using v1/customers
    `class Employees(VetspireV2Stream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalVetspireV2Stream((VetspireV2Stream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    # url_base = "https://api2.vetspire.com/graphql"
    # transformer: TypeTransformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization | TransformConfig.CustomSchemaNormalization)
    url_base = "https://api2.vetspire.com/graphql"

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self._next_page_token = None
        self.limit = kwargs.get("limit", "300")
        self.offset = kwargs.get("offset", "300")

    @property
    def data_field(self):
        return self.name

    @property
    def http_method(self) -> str:
        return "POST"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        records = response.json()['data'].get(self.object_name, [])
        if len(records) == self.limit:
            self.offset = str(int(self.limit) + int(self.offset))
            next_page_params = {"offset": str(self.offset)}
            return next_page_params

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params["PageSize"] = self.page_size
        if next_page_token:
            params.update(**next_page_token)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        records = response.json()['data'].get(self.object_name, [])
        for record in records:
            yield from record

    def _get_schema_root_properties(self):
        schema_loader = JsonFileSchemaLoader(config=self.config, parameters={"name": self.name})
        schema = schema_loader.get_json_schema()
        return schema["properties"]

    def _get_object_arguments(self, **object_arguments) -> str:
        return ",".join([f"{argument}:{value}" for argument, value in object_arguments.items() if value is not None])

    def _build_query(self, object_name: str, field_schema: dict, **object_arguments) -> str:
        """
        Recursive function that builds a GraphQL query string by traversing given stream schema properties.
        Attributes
            object_name (str): the name of root object
            field_schema (dict): configured catalog schema for current stream
            object_arguments (dict): arguments such as limit, page, ids, ... etc to be passed for given object
        """
        fields = []
        for field, nested_schema in field_schema.items():
            nested_fields = nested_schema.get("properties", nested_schema.get("items", {}).get("properties"))
            if nested_fields:
                fields.append(self._build_query(field, nested_fields))
            else:
                fields.append(field)

        arguments = self._get_object_arguments(**object_arguments)
        arguments = f"({arguments})" if arguments else ""
        fields = ",".join(fields)

        # Add offset and limit to the filter part of the query
        filter_arguments = object_arguments.get("filter", {})
        offset = filter_arguments.get("offset")
        limit = filter_arguments.get("limit")
        filter_arguments_str = ""
        if offset is not None:
            filter_arguments_str += f"offset: {offset}, "
        if limit is not None:
            filter_arguments_str += f"limit: {limit}, "
        if filter_arguments_str:
            filter_arguments_str = filter_arguments_str.rstrip(", ")

        return f"{object_name}{arguments}{{{filter_arguments_str}{fields}}}"

    def request_body_data(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Union[Mapping, str]]:
        query = self._build_query(
            object_name= self.object_name,
            field_schema= self._get_schema_root_properties(),
            limit= self.limit or None,
            offset= next_page_token,
        )
        return {"query": f"query{{{query}}}"}


class PatientPlans(VetspireV2Stream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "id"

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator)
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.config = stream_kwargs
        self._authenticator = authenticator
        self.object_name = 'patientPlans'

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return None


# Basic incremental stream
class IncrementalVetspireV2Stream(VetspireV2Stream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


class Employees(IncrementalVetspireV2Stream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    cursor_field = "start_date"

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "employee_id"

    def path(self, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return "employees"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")
