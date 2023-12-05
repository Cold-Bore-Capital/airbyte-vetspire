from abc import ABC, abstractmethod
from dataclasses import dataclass

from airbyte_cdk.models import SyncMode, AirbyteMessage
from airbyte_cdk.sources.declarative.schema.json_file_schema_loader import JsonFileSchemaLoader
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from airbyte_cdk.sources.streams.core import Stream, StreamData
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator
from airbyte_cdk.sources.streams.http.rate_limiting import default_backoff_handler, user_defined_backoff_handler
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException, RequestBodyException, UserDefinedBackoffException
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
import requests
import pendulum
from requests.auth import AuthBase
import copy


# Basic full refresh stream
# @dataclass
class VetspireV2Stream(HttpStream, ABC):
    """
    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..
    """
    transformer: TypeTransformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization | TransformConfig.CustomSchemaNormalization)
    url_base = "https://api2.vetspire.com/graphql"
    # state_checkpoint_interval = 300
    backoff_sleep = 30

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self._next_page_token = None
        self.lookback_window_days = 14

    def backoff_time(self, response: requests.Response):
        """
        The rate limit is 10 requests per minute, so we sleep
        for defined backoff_sleep time (default is 60 sec) before we continue.

        See https://support.dixa.help/en/articles/174-export-conversations-via-api
        """
        return self.backoff_sleep

    @property
    def data_field(self):
        return self.name

    @property
    def http_method(self) -> str:
        return "POST"

    @property
    def state(self) -> Mapping[str, Any]:
        # return {'offset': str(self.offset)}
        return {self.cursor_field: str(self._cursor_value)}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]
        # self._offset_value = value[self.cursor_field]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Args:
            response: the most recent response from the API
        Returns:
            If the number of records returned = limit, return the next page token. Otherwise, return None.
        Raises:
            If the response is not a valid json, raise an exception.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].
        """
        try:
            records = response.json()['data'].get(self.object_name, [])
        except:
            raise Exception(f'The json returns as follows {response.json()}')
        try:
            if len(records) == self.limit:
                self.offset = str(int(self.limit) + int(self.offset))
                next_page_params = {"offset": str(self.offset)}
                return next_page_params
        except:
            print('Need to remove offset. This is not a paginated stream.')

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Args:
            response: the most recent response from the API
        Returns:
            a list of records from the response
        Raises:
            If the response is not a valid json, raise an exception.

        """
        try:
            records = response.json()['data'].get(self.object_name, [])
        except:
            raise Exception(f'The json returns as follows {response.json()}')
        yield from records

    def _get_schema_root_properties(self):
        """
        Returns the root properties of the configured catalog schema for this stream.
        """
        schema_loader = JsonFileSchemaLoader(config={}, parameters={"name": self.name})
        schema = schema_loader.get_json_schema()
        return schema["properties"]

    def _get_object_arguments(self, **object_arguments) -> str:
        """
        The logic below is used to create the filters for the query.
        """
        object_list = []
        if len(object_arguments.items()) > 0:
            if self.locations:
                object_list.append(f"locationIds: [{','.join(self.locations)}]")

            if self.object_name == 'creditMemos':
                object_list.append("start : \"2023-04-01T00:00:00Z\"")
                object_list.append(f"end : \"{datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}\"")

            if self.object_name == 'vitals':
                object_list.append("from : \"2023-04-01T00:00:00Z\"")

            if self.object_name == 'payments':
                object_list.append("methods : [ACCOUNT_CREDIT, BAD_DEBT, REFUND]")

            if self.object_name == 'patientPlans':
                object_list.append(
                    "filters: {updatedAtStart: \"" + object_arguments["updatedAtStart"] + "\", updatedAtEnd: \"" + object_arguments[
                        "updatedAtEnd"] + "\"}")
            elif self.name == 'appointments_deleted':
                object_list.append('onlyDeleted: true')
            for k in object_arguments.keys():
                if k in ['updatedAtStart', 'updatedAtEnd','startDate','endDate'] and self.object_name != 'patientPlans':
                    object_list.append(f'{k}: "{object_arguments[k]}"')
                elif k in ['start', 'end', 'after', 'before', 'locationId'] and object_arguments[k] is not None:
                    object_list.append(f'{k}: "{object_arguments[k]}"')
                elif k in ["limit", "offset", "excludeProtocols", "excludeEmpty", "locationId"] and object_arguments[k] is not None:
                    object_list.append(f'{k}: {object_arguments[k]}')
                # else:
                #     raise Exception(f"Unknown argument {k} for object {self.object_name}")
        return ",".join(object_list)

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
            if isinstance(nested_schema, List):
                raise Exception(f"Nested lists are not supported: {nested_schema}")
            nested_fields = nested_schema.get("properties", nested_schema.get("items", {}).get("properties"))
            if nested_fields:
                fields.append(self._build_query(field, nested_fields))
            else:
                fields.append(field)

        arguments = self._get_object_arguments(**object_arguments)
        arguments = f"({arguments})" if arguments else ""
        fields = ",".join(fields)

        return f"{object_name}{arguments}{{{fields}}}"

    def request_body_data(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Union[Mapping, str]]:
        """
        Args:
            stream_state: the state of the stream
            stream_slice: the slice of the stream to read which uses dates
            next_page_token: the next page token to use (this will be none for all requests as we are using offset to paginate)
        Returns:
            The request body to be used in the POST request.

        """
        if self.object_name in ['tasks']:
            query = self._build_query(
                object_name=self.object_name,
                field_schema=self._get_schema_root_properties(),
                limit=self.limit,
                offset=self.offset,
                start=stream_slice.get('start', None),
                end=stream_slice.get('end', None)
            )
        elif 'reservations' in self.object_name:
            if pendulum.parse(stream_slice.get('startDate', None)) > pendulum.now():
                # The code below makes sure we include the last 2 weeks of reservations.
                startDate = pendulum.now() - pendulum.duration(days=self.lookback_window_days)
            else:
                startDate =  pendulum.parse(stream_slice.get('startDate', None)) - pendulum.duration(days=self.lookback_window_days)
            query = self._build_query(
                object_name=self.object_name,
                field_schema=self._get_schema_root_properties(),
                startDate=startDate.format(self.date_filter_template),
                endDate=(pendulum.now() + pendulum.duration(days=180)).format(self.date_filter_template),
                locationId=self.locationId,
            )
        elif self.object_name in ['encounterTypes', 'appointmentTypes', 'productPackages', 'preventionPlans', 'productTypes', 'providers',
                                  'locations']:
            query = self._build_query(
                object_name=self.object_name,
                field_schema=self._get_schema_root_properties(),
                limit=self.limit,
                offset=self.offset
            )
        elif self.object_name in ['appointmentTypes']:
            query = self._build_query(
                object_name=self.object_name,
                field_schema=self._get_schema_root_properties()
            )
        elif self.object_name == 'encounters':
            query = self._build_query(
                object_name=self.object_name,
                field_schema=self._get_schema_root_properties(),
                limit=self.limit,
                offset=self.offset,
                updatedAtStart=stream_slice.get('updatedAtStart', None),
                updatedAtEnd=stream_slice.get('updatedAtEnd', None)
            )
            query = query.replace('intake{label,value}','intake {... on FieldDatum {label,value}}')
            query = query.replace('sections{data{value,label}}', 'sections{name, data{ ... on ValuesetDatum {value,label}}}')
        else:
            query = self._build_query(
                object_name=self.object_name,
                field_schema=self._get_schema_root_properties(),
                limit=self.limit,
                offset=self.offset,
                updatedAtStart=stream_slice.get('updatedAtStart', None),
                updatedAtEnd=stream_slice.get('updatedAtEnd', None)
            )
        return {"query": f"query{{{query}}}"}

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        There is no path but this method needs to be instantiated.
        """
        return None

    def _read_pages(
            self,
            records_generator_fn: Callable[
                [requests.PreparedRequest, requests.Response, Mapping[str, Any], Mapping[str, Any]], Iterable[StreamData]
            ],
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[StreamData]:
        """
        This method is used to read all pages of a stream. It is used by the read_records method.
        Args:
            records_generator_fn: a function that takes in a request, response, stream_state, and stream_slice and returns a generator of records
            stream_slice: the slice of the stream to read which uses dates
            stream_state: the state of the stream
        Returns:
            A generator of records returned from the api request.
        """
        stream_state = stream_state or {}
        pagination_complete = False
        next_page_token = None

        while not pagination_complete:
            request, response = self._fetch_next_page(stream_slice, stream_state, next_page_token)
            if response.status_code in [500, 502]:
                if self.offset and response.status_code == 500:
                    self.offset = str(int(self.offset) + 1)
                    continue
                else:
                    continue  # pagination_complete = True

            yield from records_generator_fn(request, response, stream_state, stream_slice)

            if self.offset is None:
                pagination_complete = True
            # Add limit to offset to get next set of records and continue pagination
            elif self.limit:
                if len(response.json()['data'].get(self.object_name, [])) == int(self.limit):
                    self.offset = str(int(self.offset) + int(self.limit))
                else:
                    self.offset = '0'
                    pagination_complete = True
            else:
                self.offset = '0'
                pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []

    def _send(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        """
        Custom error handling that was used to deal with timeouts.
        Args:
            request: the request to be sent
            request_kwargs: the request kwargs
        Returns:
            The response from the request.
        XXXXXXXXXXXXXXXXXXXXXXXX
        Pre-created text below
        XXXXXXXXXXXXXXXXXXXXXXXX
        Wraps sending the request in rate limit and error handlers.
        Please note that error handling for HTTP status codes will be ignored if raise_on_http_errors is set to False

        This method handles two types of exceptions:
            1. Expected transient exceptions e.g: 429 status code.
            2. Unexpected transient exceptions e.g: timeout.

        To trigger a backoff, we raise an exception that is handled by the backoff decorator. If an exception is not handled by the decorator will
        fail the sync.

        For expected transient exceptions, backoff time is determined by the type of exception raised:
            1. CustomBackoffException uses the user-provided backoff value
            2. DefaultBackoffException falls back on the decorator's default behavior e.g: exponential backoff

        Unexpected transient exceptions use the default backoff parameters.
        Unexpected persistent exceptions are not handled and will cause the sync to fail.
        """
        # self.logger.debug(
        #     "Making outbound API request", extra={"headers": request.headers, "url": request.url, "request_body": request.body}
        # )
        response: requests.Response = self._session.send(request, **request_kwargs)

        # Evaluation of response.text can be heavy, for example, if streaming a large response
        # Do it only in debug mode
        if response.status_code == 500 or response.status_code == 502:  #
            return response
        if self.should_retry(response):
            custom_backoff_time = self.backoff_time(response)
            error_message = self.error_message(response)
            if custom_backoff_time:
                raise UserDefinedBackoffException(
                    backoff=custom_backoff_time, request=request, response=response, error_message=error_message
                )
            else:
                raise DefaultBackoffException(request=request, response=response, error_message=error_message)
        elif self.raise_on_http_errors:
            # Raise any HTTP exceptions that happened in case there were unexpected ones
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                self.logger.error(response.text)
                raise exc
        return response


class VetspireV2StreamWithReq(VetspireV2Stream):
    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        return params


class Providers(VetspireV2StreamWithReq):
    """
    self.object_name is used to extract the data from the json response.
    """
    primary_key = "id"
    name = 'providers'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator)
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'providers'
        self.locations = stream_kwargs.get('locations')


class AppointmentTypes(VetspireV2StreamWithReq):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """
    primary_key = "id"
    name = 'appointment_types'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator)
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'appointmentTypes'
        self.locations = stream_kwargs.get('locations')


class EncounterTypes(VetspireV2StreamWithReq):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """
    primary_key = "id"
    name = 'encounter_types'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator)
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'encounterTypes'
        self.locations = stream_kwargs.get('locations')


class Locations(VetspireV2StreamWithReq):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """
    primary_key = "id"
    name = 'locations'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator)
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'locations'
        self.locations = stream_kwargs.get('locations')



# Basic incremental stream
class IncrementalVetspireV2Stream(VetspireV2Stream, IncrementalMixin):
    """
    This class represents an incremental stream output by the connector.
    """
    datetime_filter_template = "YYYY-MM-DDTHH:mm:ssZ"
    date_filter_template = "YYYY-MM-DD"
    slice_granularity = pendulum.duration(microseconds=1)
    # state_checkpoint_interval = 300
    sync_mode = SyncMode.incremental
    _slice_step = pendulum.duration(days=1)

    def __init__(
            self,
            authenticator: Union[AuthBase, HttpAuthenticator],
            start_datetime: str = None,
            _slice_step = pendulum.duration(days=1),
            slice_step_map: Mapping[str, int] = None
    ):
        super().__init__(authenticator)
        self._start_datetime = pendulum.parse(start_datetime if start_datetime is not None else "2023-07-01T00:00:00Z")

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        return {}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = pendulum.parse(value[self.cursor_field]).in_timezone('UTC')

    @property
    def sync_mode(self):
        return SyncMode.incremental

    @property
    def supported_sync_modes(self):
        return [SyncMode.incremental]

    @property
    def slice_step(self):
        return self._slice_step  # or self.slice_step_default

    @property
    # @abstractmethod
    def lower_boundary_filter_field(self) -> str:
        """
        return: date filter query parameter name
        """

    @property
    # @abstractmethod
    def upper_boundary_filter_field(self) -> str:
        """
        return: date filter query parameter name
        """

    def generate_date_ranges(self) -> Iterable[Optional[MutableMapping[str, Any]]]:
        end_datetime = pendulum.now("utc")
        start_datetime = min(end_datetime, self.state.get(self.cursor_field, self._start_datetime))

        current_start = start_datetime
        current_end = start_datetime
        # Aligning to a datetime format is done to avoid the following scenario:
        # start_dt = 2021-11-14T00:00:00, end_dt (now) = 2022-11-14T12:03:01, time_filter_template = "YYYY-MM-DD"
        # First slice: (2021-11-14, 2022-11-14)
        # (!) Second slice: (2022-11-15, 2022-11-14) - because 2022-11-14T00:00:00 (prev end) < 2022-11-14T12:03:01,
        # so we have to compare dates, not date-times to avoid yielding that last slice
        while current_end < end_datetime:
            current_end = min(end_datetime, current_start + self.slice_step)
            if (self.object_name in ['tasks']):
                slice_ = {
                    self.lower_boundary_filter_field: current_start.format(self.date_filter_template),
                    self.upper_boundary_filter_field: current_end.format(self.date_filter_template),
                }
            else:
                slice_ = {
                    self.lower_boundary_filter_field: current_start.format(self.datetime_filter_template),
                    self.upper_boundary_filter_field: current_end.format(self.datetime_filter_template),
                }
            yield slice_
            current_start = current_end + self.slice_granularity

    def stream_slices(
            self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for super_slice in super().stream_slices(sync_mode=sync_mode, cursor_field=cursor_field, stream_state=stream_state):
            for dt_range in self.generate_date_ranges():
                slice_ = copy.deepcopy(super_slice) if super_slice else {}
                slice_.update(dt_range)
                yield slice_

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        lower_bound = stream_slice and stream_slice.get(self.lower_boundary_filter_field)
        upper_bound = stream_slice and stream_slice.get(self.upper_boundary_filter_field)
        if lower_bound:
            params[self.lower_boundary_filter_field] = lower_bound
        if upper_bound:
            params[self.upper_boundary_filter_field] = upper_bound
        return params

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: List[str] = None,
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        stream_state = {}
        unsorted_records = []
        for record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state):
            record[self.cursor_field] = pendulum.parse(record[self.cursor_field], strict=False).to_iso8601_string()
            unsorted_records.append(record)
        sorted_records = sorted(unsorted_records, key=lambda x: x[self.cursor_field])
        for record in sorted_records:
            if isinstance(record[self.cursor_field], str):
                record[self.cursor_field] = pendulum.parse(record[self.cursor_field])

            if record[self.cursor_field] >= self.state.get(self.cursor_field, self._start_datetime):
                self._cursor_value = record[self.cursor_field]
                yield record


class PatientPlans(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    sync_mode = SyncMode.incremental
    name = 'patient_plans'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'patientPlans'
        self.locations = stream_kwargs.get('locations')


class ConversationsPaginated(IncrementalVetspireV2Stream):
    cursor_field = "insertedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "start"
    upper_boundary_filter_field = "end"
    sync_mode = SyncMode.incremental
    name = 'conversations_paginated'
    slice_step = pendulum.duration(years=1)

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'conversationsPaginated'
        self.locations = stream_kwargs.get('locations')


class Appointments(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    sync_mode = SyncMode.incremental
    name = 'appointments'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'appointments'
        self.locations = stream_kwargs.get('locations')


class AppointmentsDeleted(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    sync_mode = SyncMode.incremental
    name = 'appointments_deleted'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'appointments'
        self.locations = stream_kwargs.get('locations')


class Clients(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'clients'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'clients'
        self.locations = stream_kwargs.get('locations')


class Encounters(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'encounters'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'encounters'
        self.locations = None


class Orders(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'orders'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = 100
        self.object_name = 'orders'
        self.locations = stream_kwargs.get('locations')


class OrderItems(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'order_items'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'orderItems'
        self.locations = stream_kwargs.get('locations')


class Patients(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'patients'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'patients'
        self.locations = stream_kwargs.get('locations')


class Payments(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'payments'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'payments'
        self.locations = stream_kwargs.get('locations')


class Products(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'products'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'products'
        self.locations = stream_kwargs.get('locations')

class PatientProtocols(IncrementalVetspireV2Stream):
    cursor_field = "dueDate" # "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'patient_protocols'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'patientProtocols'
        self.locations = stream_kwargs.get('locations')


class PreventionPlans(IncrementalVetspireV2Stream):
    cursor_field = "insertedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'prevention_plans'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'preventionPlans'
        self.locations = stream_kwargs.get('locations')


class ProductTypes(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'product_types'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = None
        self.limit = None
        self.object_name = 'productTypes'
        self.locations = stream_kwargs.get('locations')


class ProductPackages(IncrementalVetspireV2Stream):
    cursor_field = "insertedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'product_packages'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'productPackages'
        self.locations = stream_kwargs.get('locations')


class Vitals(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'vitals'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset =  stream_kwargs.get('offset')
        self.limit =  stream_kwargs.get('limit')
        self.object_name = 'vitals'
        self.locations = None


class CreditMemos(IncrementalVetspireV2Stream):
    cursor_field = "datetime"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'credit_memos'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'creditMemos'
        self.locations = stream_kwargs.get('locations')


class Tasks(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "start"
    upper_boundary_filter_field = "end"
    name = 'tasks'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.offset = stream_kwargs.get('offset')
        self.limit = stream_kwargs.get('limit')
        self.object_name = 'tasks'
        self.locations = stream_kwargs.get('locations')


class Reservations_DAB010(IncrementalVetspireV2Stream):
    name = 'reservations_DAB010'
    cursor_field = "startDate"
    _slice_step = pendulum.duration(days=365)
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "startDate"
    upper_boundary_filter_field = "endDate"

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.locations = None
        self.object_name = 'reservations'
        self.limit = None
        self.offset=None
        self.locationId = stream_kwargs.get('locationId')



class Reservations_DAB011(IncrementalVetspireV2Stream):
    name = 'reservations_DAB011'
    cursor_field = "startDate"
    _slice_step = pendulum.duration(days=365)
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "startDate"
    upper_boundary_filter_field = "endDate"

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.locations = None
        self.object_name = 'reservations'
        self.limit = None
        self.offset=None
        self.locationId = stream_kwargs.get('locationId')


class Reservations_DAB012(IncrementalVetspireV2Stream):
    name = 'reservations_DAB012'
    cursor_field = "startDate"
    _slice_step = pendulum.duration(days=365)
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "startDate"
    upper_boundary_filter_field = "endDate"

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.locations = None
        self.object_name = 'reservations'
        self.limit = None
        self.offset=None
        self.locationId = stream_kwargs.get('locationId')


class Reservations_DFW012(IncrementalVetspireV2Stream):
    name = 'reservations_DFW012'
    cursor_field = "startDate"
    _slice_step = pendulum.duration(days=365)
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "startDate"
    upper_boundary_filter_field = "endDate"

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.locations = None
        self.object_name = 'reservations'
        self.limit = None
        self.offset=None
        self.locationId = stream_kwargs.get('locationId')


class Reservations_DFW010(IncrementalVetspireV2Stream):
    name = 'reservations_DFW010'
    cursor_field = "startDate"
    _slice_step = pendulum.duration(days=365)
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "startDate"
    upper_boundary_filter_field = "endDate"

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs.get('start_datetime'))
        self.locations = None
        self.object_name = 'reservations'
        self.limit = None
        self.offset=None
        self.locationId = stream_kwargs.get('locationId')


