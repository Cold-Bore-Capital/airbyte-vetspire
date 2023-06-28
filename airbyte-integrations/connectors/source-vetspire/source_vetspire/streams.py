import math
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator

from abc import ABC, abstractmethod
from itertools import chain
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Type, Union, Callable
from airbyte_cdk.sources.streams.core import Stream, StreamData
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import SimpleRetriever
from collections import OrderedDict
from airbyte_cdk.utils.airbyte_secrets_utils import filter_secrets
from airbyte_cdk.sources.streams import Stream
import pendulum
import requests
from airbyte_cdk.models import AirbyteLogMessage, AirbyteMessage, Level, SyncMode, Type as MessageType
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from airbyte_cdk.sources.declarative.schema.json_file_schema_loader import JsonFileSchemaLoader
# from source_stripe.availability_strategy import StripeSubStreamAvailabilityStrategy
from airbyte_cdk.sources.streams.http.requests_native_auth import BasicHttpAuthenticator
from airbyte_cdk.sources.declarative.extractors.http_selector import HttpSelector
from airbyte_cdk.sources.declarative.types import StreamSlice, StreamState
import os
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator
from requests.auth import AuthBase
from airbyte_cdk.sources.declarative.exceptions import ReadException
from airbyte_cdk.sources.declarative.requesters.requester import Requester
from airbyte_cdk.sources.declarative.retrievers.retriever import Retriever
from airbyte_cdk.sources.declarative.partition_routers.single_partition_router import SinglePartitionRouter
from airbyte_cdk.sources.declarative.stream_slicers.stream_slicer import StreamSlicer
from airbyte_cdk.sources.declarative.types import Config, Record, StreamSlice, StreamState
from airbyte_cdk.sources.declarative.requesters.error_handlers.response_action import ResponseAction
from airbyte_cdk.sources.declarative.requesters.paginators.no_pagination import NoPagination
from airbyte_cdk.sources.declarative.requesters.paginators.paginator import Paginator
from dataclasses import InitVar, dataclass, field
from json import JSONDecodeError
import json
import time
import logging

#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, List, Mapping

import requests
from airbyte_cdk.sources.declarative.decoders.decoder import Decoder
from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.types import Record


# Functions
def _prepared_request_to_airbyte_message(request: requests.PreparedRequest) -> AirbyteMessage:
    # FIXME: this should return some sort of trace message
    request_dict = {
        "url": request.url,
        "http_method": request.method,
        "headers": dict(request.headers),
        "body": _body_binary_string_to_dict(request.body),
    }
    log_message = filter_secrets(f"request:{json.dumps(request_dict)}")
    return AirbyteMessage(type=MessageType.LOG, log=AirbyteLogMessage(level=Level.INFO, message=log_message))


def _body_binary_string_to_dict(body_str: str) -> Optional[Mapping[str, str]]:
    if body_str:
        if isinstance(body_str, (bytes, bytearray)):
            body_str = body_str.decode()
        try:
            return json.loads(body_str)
        except JSONDecodeError:
            return {k: v for k, v in [s.split("=") for s in body_str.split("&")]}
    else:
        return None


def _response_to_airbyte_message(response: requests.Response) -> AirbyteMessage:
    # FIXME: this should return some sort of trace message
    response_dict = {"body": response.text, "headers": dict(response.headers), "status_code": response.status_code}
    log_message = filter_secrets(f"response:{json.dumps(response_dict)}")
    return AirbyteMessage(type=MessageType.LOG, log=AirbyteLogMessage(level=Level.INFO, message=log_message))


@dataclass
class VetspireExtractor(RecordExtractor):
    """
    Record extractor that extracts record of the form:

    { "list": { "ID_1": record_1, "ID_2": record_2, ... } }

    Attributes:
        parameters (Mapping[str, Any]): Additional runtime parameters to be used for string interpolation
        decoder (Decoder): The decoder responsible to transfom the response in a Mapping
        field_path (str): The field defining record Mapping
    """

    parameters: InitVar[Mapping[str, Any]]
    decoder: Decoder = JsonDecoder(parameters={})
    field_path: str = "list"
    use_cache = False

    @staticmethod
    def get_authenticator(config: Mapping[str, Any]):
        return AuthenticatorWithRateLimiter(api_key=config.get("api_key"), auth_method="Authorization")

    def extract_records(self, response: requests.Response) -> List[Record]:
        response_body = self.decoder.decode(response)
        if self.field_path not in response_body:
            return []
        elif type(response_body[self.field_path]) is list:
            return response_body[self.field_path]
        else:
            return [record for _, record in response_body[self.field_path].items()]

# class VetspireRetriever(Retriever):


@dataclass
class VetspireStream(Retriever, HttpStream, ABC):
    url_base = "https://api2.vetspire.com/graphql"
    primary_key = "id"
    transformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)
    requester: Requester
    record_selector: HttpSelector
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    name: str
    _name: Union[InterpolatedString, str] = field(init=False, repr=False, default="")
    primary_key: Optional[Union[str, List[str], List[List[str]]]]
    _primary_key: str = field(init=False, repr=False, default="")
    paginator: Optional[Paginator] = None
    stream_slicer: Optional[StreamSlicer] = SinglePartitionRouter(parameters={})
    use_cache = False

    @property
    def name(self) -> str:
        """
        :return: Stream name
        """
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        if not isinstance(value, property):
            self._name = value

    @property
    def url_base(self) -> str:
        return "https://api2.vetspire.com/graphql"

    @property
    def http_method(self) -> str:
        return str(self.requester.get_method().value)

    @property
    def raise_on_http_errors(self) -> bool:
        # never raise on http_errors because this overrides the error handler logic...
        return False

    @property
    def cache_filename(self) -> str:
        """
        Return the name of cache file
        """
        return self.requester.cache_filename

    @property
    def use_cache(self) -> bool:
        """
        If True, all records will be cached.
        """
        return self.requester.use_cache

    def __post_init__(self, parameters: Mapping[str, Any]):
        self.paginator = self.paginator or NoPagination(parameters=parameters)
        HttpStream.__init__(self, self.requester.get_authenticator(self.config))
        self._last_response = None
        self._last_records = None
        self._parameters = parameters
        self.name = InterpolatedString(self._name, parameters=parameters)


    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: StreamState,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Record]:
        # if fail -> raise exception
        # if ignore -> ignore response and return no records
        # else -> delegate to record selector
        response_status = self.requester.interpret_response_status(response)
        if response_status.action == ResponseAction.FAIL:
            error_message = (
                response_status.error_message
                or f"Request to {response.request.url} failed with status code {response.status_code} and error message {HttpStream.parse_response_error_message(response)}"
            )
            raise ReadException(error_message)
        elif response_status.action == ResponseAction.IGNORE:
            self.logger.info(f"Ignoring response for failed request with error message {HttpStream.parse_response_error_message(response)}")
            return []

        # Warning: use self.state instead of the stream_state passed as argument!
        self._last_response = response
        records = self.record_selector.select_records(
            response=response, stream_state=self.state, stream_slice=stream_slice, next_page_token=next_page_token
        )
        self._last_records = records
        return records

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """The stream's primary key"""
        return self._primary_key

    @primary_key.setter
    def primary_key(self, value: str) -> None:
        if not isinstance(value, property):
            self._primary_key = value

    def should_retry(self, response: requests.Response) -> bool:
        """
        Specifies conditions for backoff based on the response from the server.

        By default, back off on the following HTTP response statuses:
         - 429 (Too Many Requests) indicating rate limiting
         - 500s to handle transient server errors

        Unexpected but transient exceptions (connection timeout, DNS resolution failed, etc..) are retried by default.
        """
        return self.requester.interpret_response_status(response).action == ResponseAction.RETRY

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """
        Specifies backoff time.

         This method is called only if should_backoff() returns True for the input request.

         :param response:
         :return how long to backoff in seconds. The return value may be a floating point number for subsecond precision. Returning None defers backoff
         to the default backoff behavior (e.g using an exponential algorithm).
        """
        should_retry = self.requester.interpret_response_status(response)
        if should_retry.action != ResponseAction.RETRY:
            raise ValueError(f"backoff_time can only be applied on retriable response action. Got {should_retry.action}")
        assert should_retry.action == ResponseAction.RETRY
        return should_retry.retry_in

    def error_message(self, response: requests.Response) -> str:
        """
        Constructs an error message which can incorporate the HTTP response received from the partner API.

        :param response: The incoming HTTP response from the partner API
        :return The error message string to be emitted
        """
        return self.requester.interpret_response_status(response).error_message

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: List[str] = None,
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        try:
            yield from super()._read_pages(
                # yield from self._read_pages(
                lambda req, res, state, _slice: self.parse_response(res, stream_slice=_slice, stream_state=state), stream_slice,
                # lambda req, res, _slice: self.parse_response(res, stream_slice=_slice), stream_slice
            )
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            parsed_error = e.response.json()
            error_code = parsed_error.get("error", {}).get("code")
            error_message = parsed_error.get("message")
            # if the API Key doesn't have required permissions to particular stream, this stream will be skipped
            if status_code == 403:
                self.logger.warn(f"Stream {self.name} is skipped, due to {error_code}. Full message: {error_message}")
                pass
            else:
                self.logger.error(f"Syncing stream {self.name} is failed, due to {error_code}. Full message: {error_message}")

    def stream_slices(
            self, *, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Optional[StreamState] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Specifies the slices for this stream. See the stream slicing section of the docs for more information.

        :param sync_mode:
        :param cursor_field:
        :param stream_state:
        :return:
        """
        # Warning: use self.state instead of the stream_state passed as argument!
        return self.stream_slicer.stream_slices(sync_mode, self.state)

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self.stream_slicer.get_stream_state()

    @state.setter
    def state(self, value: StreamState):
        """State setter, accept state serialized by state getter."""
        self.stream_slicer.update_cursor(value)

    def _parse_records_and_emit_request_and_responses(self, request, response, stream_state, stream_slice) -> Iterable[StreamData]:
        # Only emit requests and responses when running in debug mode
        if self.logger.isEnabledFor(logging.DEBUG):
            yield _prepared_request_to_airbyte_message(request)
            yield _response_to_airbyte_message(response)
        # Not great to need to call _read_pages which is a private method
        # A better approach would be to extract the HTTP client from the HttpStream and call it directly from the HttpRequester
        yield from self.parse_response(response, stream_slice=stream_slice, stream_state=stream_state)

    def _read_pages(
            self,
            records_generator_fn: Callable[
                [requests.PreparedRequest, requests.Response, Mapping[str, Any], Mapping[str, Any]], Iterable[StreamData]
            ],
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[StreamData]:
        stream_state = stream_state or {}
        pagination_complete = False
        next_page_token = None
        while not pagination_complete:
            request, response = self._fetch_next_page(stream_slice, stream_state, next_page_token)
            yield from records_generator_fn(request, response, stream_state, stream_slice)

            next_page_token = self.next_page_token(response)
            if not next_page_token:
                pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []

    def _fetch_next_page(
            self, stream_slice: Mapping[str, Any] = None, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Tuple[requests.PreparedRequest, requests.Response]:
        request_headers = self.request_headers(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        request = self._create_prepared_request(
            path=self.path(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
            headers=dict(request_headers, **self.authenticator.get_auth_header()),
            params=self.request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
            json=self.request_body_json(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
            # data=self.get(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
            data=self.request_body_data(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
        )
        request_kwargs = self.request_kwargs(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        response = self._send_request(request, request_kwargs)

        return request, response

    @property
    def http_method(self) -> str:
        return "POST"

    @property
    def data_field(self) -> str:
        """
        :return: Default field name to get data from response
        """
        return self.name

    def backoff_time(self, response: requests.Response):
        if "Retry-After" in response.headers:
            return int(response.headers["Retry-After"])
        else:
            self.logger.info("Retry-after header not found. Using default backoff value")
            return super().backoff_time(response)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        stream_data = response.json()

        # depending on stream type we may get either:
        # * nested records iterable in response object;
        # * not nested records iterable;
        # * single object to yield.
        if self.data_field:
            stream_data = response.json()['data'].get(self.data_field, [])

        if isinstance(stream_data, list):
            yield from stream_data
        else:
            yield stream_data

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        if decoded_response['data'][self.name] == self.limit:
            self.offset += self.limit
            return {"offset": self.offset}
        # if "has_more" in decoded_response and decoded_response["has_more"] and decoded_response.get("data", []):
        #     last_object_id = decoded_response["data"][-1]["id"]
        #     return {"starting_after": last_object_id}

    def request_params(
        self,
        stream_state: StreamSlice,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        """
        Specifies the query parameters that should be set on an outgoing HTTP request given the inputs.

        E.g: you might want to define query parameters for paging if next_page_token is not None.
        """
        return self._get_request_options(
            stream_slice,
            next_page_token,
            self.requester.get_request_params,
            self.paginator.get_request_params,
            self.stream_slicer.get_request_params,
        )

    def request_body_data(
        self,
        stream_state: StreamState,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Union[Mapping, str]]:
        """
        Specifies how to populate the body of the request with a non-JSON payload.

        If returns a ready text that it will be sent as is.
        If returns a dict that it will be converted to a urlencoded form.
        E.g. {"key1": "value1", "key2": "value2"} => "key1=value1&key2=value2"

        At the same time only one of the 'request_body_data' and 'request_body_json' functions can be overridden.
        """
        # Warning: use self.state instead of the stream_state passed as argument!
        base_body_data = self.requester.get_request_body_data(
            stream_state=self.state, stream_slice=stream_slice, next_page_token=next_page_token
        )
        if isinstance(base_body_data, str):
            paginator_body_data = self.paginator.get_request_body_data()
            if paginator_body_data:
                raise ValueError(
                    f"Cannot combine requester's body data= {base_body_data} with paginator's body_data: {paginator_body_data}"
                )
            else:
                return base_body_data
        return self._get_request_options(
            stream_slice,
            next_page_token,
            self.requester.get_request_body_data,
            self.paginator.get_request_body_data,
            self.stream_slicer.get_request_body_data,
        )


    # def get_request_body_data(self, *args, **kwargs) -> Optional[Union[Mapping, str]]:
    # def request_body_datat(
    def get_request_body_data(
            self,
            stream_state: StreamState,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Union[Mapping, str]]:
        """
        Combines queries to a single GraphQL query.
        """
        limit = self.limit
        # offset = next_page_token and next_page_token[self.next_page_offset]
        offset = 0
        # page = next_page_token and next_page_token[self.NEXT_PAGE_TOKEN_FIELD_NAME]
        query = self._build_query(
            object_name=self.query_object,
            field_schema=self._get_schema_root_properties(),
            limit=limit or None,
            offset=offset or None,
        )
        return {"query": f"query{{{query}}}"}

    def _get_request_options(
        self,
        stream_slice: Optional[StreamSlice],
        next_page_token: Optional[Mapping[str, Any]],
        requester_method,
        paginator_method,
        stream_slicer_method,
    ):
        """
        Get the request_option from the requester and from the paginator
        Raise a ValueError if there's a key collision
        Returned merged mapping otherwise
        :param stream_slice:
        :param next_page_token:
        :param requester_method:
        :param paginator_method:
        :return:
        """

        requester_mapping = requester_method(stream_state=self.state, stream_slice=stream_slice, next_page_token=next_page_token)
        requester_mapping_keys = set(requester_mapping.keys())
        paginator_mapping = paginator_method(stream_state=self.state, stream_slice=stream_slice, next_page_token=next_page_token)
        paginator_mapping_keys = set(paginator_mapping.keys())
        stream_slicer_mapping = stream_slicer_method(stream_slice=stream_slice)
        stream_slicer_mapping_keys = set(stream_slicer_mapping.keys())

        intersection = (
            (requester_mapping_keys & paginator_mapping_keys)
            | (requester_mapping_keys & stream_slicer_mapping_keys)
            | (paginator_mapping_keys & stream_slicer_mapping_keys)
        )
        if intersection:
            raise ValueError(f"Duplicate keys found: {intersection}")
        return {**requester_mapping, **paginator_mapping, **stream_slicer_mapping}

    # def request_headers(
    #     self, stream_state: StreamState, stream_slice: Optional[StreamSlice] = None, next_page_token: Optional[Mapping[str, Any]] = None
    # ) -> Mapping[str, Any]:
    #     """
    #     Specifies request headers.
    #     Authentication headers will overwrite any overlapping headers returned from this method.
    #     """
    #     headers = self._get_request_options(
    #         stream_slice,
    #         next_page_token,
    #         self.requester.get_request_headers,
    #         self.paginator.get_request_headers,
    #         self.stream_slicer.get_request_headers,
    #     )
    #     return {str(k): str(v) for k, v in headers.items()}


    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("data", [])  # Stripe puts records in a container array "data"

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

    def _get_object_arguments(self, **object_arguments) -> str:
        return ",".join([f"{argument}:{value}" for argument, value in object_arguments.items() if value is not None])

    def _get_schema_root_properties(self):
        schema_loader = JsonFileSchemaLoader(config=self.config, parameters={"name": self.name})
        schema = schema_loader.get_json_schema()
        return schema["properties"]

    # def read_records(
    #     self,
    #     sync_mode: SyncMode,
    #     cursor_field: List[str] = None,
    #     stream_slice: Mapping[str, Any] = None,
    #     stream_state: Mapping[str, Any] = None,
    # ) -> Iterable[Mapping[str, Any]]:
    #     try:
    #         yield from super().read_records(sync_mode, cursor_field, stream_slice, stream_state)
    #     except requests.exceptions.HTTPError as e:
    #         status_code = e.response.status_code
    #         parsed_error = e.response.json()
    #         error_code = parsed_error.get("error", {}).get("code")
    #         error_message = parsed_error.get("message")
    #         # if the API Key doesn't have required permissions to particular stream, this stream will be skipped
    #         if status_code == 403 and error_code in STRIPE_ERROR_CODES:
    #             self.logger.warn(f"Stream {self.name} is skipped, due to {error_code}. Full message: {error_message}")
    #             pass
    #         else:
    #             self.logger.error(f"Syncing stream {self.name} is failed, due to {error_code}. Full message: {error_message}")


class IncrementalHarvestStream(VetspireStream, ABC):
    cursor_field = "updated_at"

    def __init__(self, replication_start_date: pendulum.datetime = None, **kwargs):
        super().__init__(**kwargs)
        self._replication_start_date = replication_start_date

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Return the latest state by comparing the cursor value in the latest record with the stream's most recent state object
        and returning an updated state object.
        """
        latest_benchmark = latest_record[self.cursor_field]
        if current_stream_state.get(self.cursor_field):
            return {self.cursor_field: max(latest_benchmark, current_stream_state[self.cursor_field])}
        return {self.cursor_field: latest_benchmark}

    def get_start_timestamp(self, stream_state) -> int:
        start_point = self.start_date
        if stream_state and self.cursor_field in stream_state:
            start_point = max(start_point, stream_state[self.cursor_field])

        if start_point and self.lookback_window_days:
            self.logger.info(f"Applying lookback window of {self.lookback_window_days} days to stream {self.name}")
            start_point = int(pendulum.from_timestamp(start_point).subtract(days=abs(self.lookback_window_days)).timestamp())

        return start_point

    def get_request_body_data(self, *args, **kwargs) -> Optional[Union[Mapping, str]]:
        """
        Combines queries to a single GraphQL query.
        """
        limit = self.config['limit']
        # offset = next_page_token and next_page_token[self.next_page_offset]
        offset = 0
        query = self._build_query(
            object_name=self.name_,
            field_schema=self._get_schema_root_properties(),
            limit=limit or None,
            offset=offset or None,
        )
        return {"query": f"query{{{query}}}"}


class patient_plans(VetspireStream):
    def __init__(self, **kwargs):
        super().__init__(requester =VetspireExtractor ,  **kwargs)
        self.api_key = kwargs['config']['authenticator']
        self.start_datetime = kwargs['config']['start_datetime']
        self.start_date = kwargs['config']['start_date']
        self.query_object = 'paitentPlans'
        self.name = 'patient_plans'

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.url_base



class AuthenticatorWithRateLimiter(BasicHttpAuthenticator):
    """
    Each token in the cycle is checked against the rate limiter.
    If a token exceeds the capacity limit, the system switches to another token.
    If all tokens are exhausted, the system will enter a sleep state until
    the first token becomes available again.
    """

    DURATION = 3600  # seconds

    def __init__(self, api_key: str, auth_method: str = "Bearer", auth_header: str = "Authorization"):
        self._auth_method = auth_method
        self._auth_header = auth_header

    @property
    def auth_header(self) -> str:
        return self._auth_header

    @property
    def token(self) -> str:
        while True:
            token = next(self._tokens_iter)
            if self._check_token(token):
                return f"{self._auth_method} {token}"

    def _check_token(self, token: str):
        """check that token is not limited"""
        self._refill()
        if self._sleep():
            self._refill()
        if self._tokens[token].count > 0:
            self._tokens[token].count -= 1
            return True

    def _refill(self):
        """refill all needed tokens"""
        now = time.time()
        for token, ns in self._tokens.items():
            if now - ns.update_at >= self.DURATION:
                ns.update_at = now
                ns.count = self._requests_per_hour

    def _sleep(self):
        """sleep only if all tokens is exhausted"""
        now = time.time()
        if sum([ns.count for ns in self._tokens.values()]) == 0:
            sleep_time = self.DURATION - (now - min([ns.update_at for ns in self._tokens.values()]))
            logging.warning("Sleeping for %.1f seconds to enforce the limit of %d requests per hour.", sleep_time, self._requests_per_hour)
            time.sleep(sleep_time)
            return True

# Source
class SourceVetspire(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None


    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        config = {
            "authenticator": config['api_key'] ,
            "start_datetime": config["start_datetime"],
            "start_date": config["start_date"]
        }
        parameters = {"limit": 300, "offset":0}
        return [patient_plans(config=config, record_selector=VetspireExtractor, parameters=parameters)]
