from abc import ABC, abstractmethod
from dataclasses import dataclass

from airbyte_cdk.models import SyncMode, AirbyteMessage
from airbyte_cdk.sources.declarative.schema.json_file_schema_loader import JsonFileSchemaLoader
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from airbyte_cdk.sources.streams.core import Stream, StreamData
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
import requests
import pendulum
from requests.auth import AuthBase
import copy
import time

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
    transformer: TypeTransformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization | TransformConfig.CustomSchemaNormalization)
    url_base = "https://api2.vetspire.com/graphql"
    state_checkpoint_interval = 300

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self._next_page_token = None

    @property
    def data_field(self):
        return self.name

    @property
    def http_method(self) -> str:
        return "POST"

    @property
    def state(self) -> Mapping[str, Any]:
        #return {'offset': str(self.offset)}
        return {self.cursor_field: str(self._cursor_value)}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]
        # self._offset_value = value[self.cursor_field]

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

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        records = response.json()['data'].get(self.object_name, [])
        yield from records

    def _get_schema_root_properties(self):
        schema_loader = JsonFileSchemaLoader(config={}, parameters={"name": self.name})
        schema = schema_loader.get_json_schema()
        return schema["properties"]

    def _get_object_arguments(self, **object_arguments) -> str:
        object_list = []
        if len(object_arguments.items()) > 0:
            if self.object_name == 'patientPlans':
                object_list.append("filters: {startAfter: \"" + object_arguments["startAfter"] + "\", startBefore: \"" + object_arguments["startBefore"]+"\"}")
            for k in object_arguments.keys():
                if k in ['updatedAtStart', 'updatedAtEnd']:
                    object_list.append(f'{k}: "{object_arguments[k]}"')
                elif k in ["limit", "offset"] and object_arguments[k] is None:
                    pass
                elif k in ["limit", "offset"]:
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

        if self.object_name == 'patientPlans':
            query = self._build_query(
            object_name=self.object_name,
            field_schema=self._get_schema_root_properties(),
            limit=self.limit,
            offset=self.offset,
            startAfter=stream_slice.get('startAfter', None),
            startBefore=stream_slice.get('startBefore', None)
        )
        elif self.object_name in ['encounterTypes','appointmentTypes','productPackages','preventionPlans','productTypes','providers','locations']:
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
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
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
        stream_state = stream_state or {}
        pagination_complete = False
        next_page_token = None
        while not pagination_complete:
            request, response = self._fetch_next_page(stream_slice, stream_state, next_page_token)

            yield from records_generator_fn(request, response, stream_state, stream_slice)

            # next_page_token = self.next_page_token(response)
            # Check if self.limit is being used. For example, ProductPackages doesn't have a limit argument.
            # try:
            if response.status_code == 500:
                time.sleep(20)
            # Offset is not being used so set pagination to complete
            elif self.offset is None:
                pagination_complete = True
            # Add limit to offset to get next set of records and continue pagination
            elif len(response.json()['data'].get(self.object_name, [])) == int(self.limit):
                self.offset = str(int(self.offset) + int(self.limit))
            else:
                self.offset = '0'
                pagination_complete = True
            # except:
            #     pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []


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
    TODO: Change class name to match the table/data source this stream corresponds to.
    """
    primary_key = "id"
    name = 'providers'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator)
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'providers'

class AppointmentTypes(VetspireV2StreamWithReq):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """
    primary_key = "id"
    name = 'appointment_types'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator)
        self.offset = None
        self.limit = None
        self.object_name = 'appointmentTypes'

class EncounterTypes(VetspireV2StreamWithReq):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """
    primary_key = "id"
    name = 'encounter_types'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator)
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'encounterTypes'

class Locations(VetspireV2StreamWithReq):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """
    primary_key = "id"
    name = 'locations'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator)
        self.offset = None
        self.limit = None
        self.object_name = 'locations'


# Basic incremental stream
class IncrementalVetspireV2Stream(VetspireV2Stream, IncrementalMixin):
    time_filter_template = "YYYY-MM-DDTHH:mm:ssZ"
    time_filter_template_patient_plans = "YYYY-MM-DD"
    # This attribute allows balancing between sync speed and memory consumption.
    # The greater a slice is - the bigger memory consumption and the faster syncs are since fewer requests are made.
    slice_step_default = pendulum.duration(days=1)
    # time gap between when previous slice ends and current slice begins
    slice_granularity = pendulum.duration(microseconds=1)
    state_checkpoint_interval = 300
    sync_mode = SyncMode.incremental

    def __init__(
            self,
            authenticator: Union[AuthBase, HttpAuthenticator],
            start_datetime: str = None,
            slice_step_map: Mapping[str, int] = None
    ):
        super().__init__(authenticator)
        slice_step = (slice_step_map or {}).get(self.name)
        self._slice_step = slice_step and pendulum.duration(days=slice_step)
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
        return self._slice_step or self.slice_step_default

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
            if self.object_name == 'patientPlans':
                slice_ = {
                    self.lower_boundary_filter_field: current_start.format(self.time_filter_template_patient_plans),
                    self.upper_boundary_filter_field: current_end.format(self.time_filter_template_patient_plans),
                }
            else:
                slice_ = {
                    self.lower_boundary_filter_field: current_start.format(self.time_filter_template),
                    self.upper_boundary_filter_field: current_end.format(self.time_filter_template),
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
        unsorted_records = []
        for record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state):
            record[self.cursor_field] = pendulum.parse(record[self.cursor_field], strict=False).to_iso8601_string()
            unsorted_records.append(record)
        sorted_records = sorted(unsorted_records, key=lambda x: x[self.cursor_field])
        for record in sorted_records:
            if isinstance(record[self.cursor_field],str):
                record[self.cursor_field] = pendulum.parse(record[self.cursor_field])

            if record[self.cursor_field] >= self.state.get(self.cursor_field, self._start_datetime):
                self._cursor_value = record[self.cursor_field]
                yield record

class PatientPlans(IncrementalVetspireV2Stream):
    cursor_field = "startDate"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "startAfter"
    upper_boundary_filter_field = "startBefore"
    sync_mode = SyncMode.incremental
    name = 'patient_plans'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs['start_datetime'][:10])
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'patientPlans'

class Appointments(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    sync_mode = SyncMode.incremental
    name = 'appointments'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs['start_datetime'])
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'appointments'


class Clients(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'clients'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs['start_datetime'])
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'clients'


class Encounters(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'encounters'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs['start_datetime'])
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'encounters'


class Orders(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'orders'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs['start_datetime'])
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'orders'


class Patients(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'patients'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs['start_datetime'])
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'patients'

class Payments(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'payments'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs['start_datetime'])
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'payments'

class Products(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'products'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs['start_datetime'])
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'products'


class PreventionPlans(IncrementalVetspireV2Stream):
    cursor_field = "insertedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'prevention_plans'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs['start_datetime'])
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'preventionPlans'


class ProductTypes(IncrementalVetspireV2Stream):
    cursor_field = "updatedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'product_types'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs['start_datetime'])
        self.offset = None
        self.limit = None
        self.object_name = 'productTypes'


class ProductPackages(IncrementalVetspireV2Stream):
    cursor_field = "insertedAt"
    _cursor_value = None
    primary_key = "id"
    lower_boundary_filter_field = "updatedAtStart"
    upper_boundary_filter_field = "updatedAtEnd"
    name = 'product_packages'

    def __init__(self, authenticator, **stream_kwargs):
        super().__init__(authenticator=authenticator, start_datetime=stream_kwargs['start_datetime'])
        self.offset = stream_kwargs['offset']
        self.limit = stream_kwargs['limit']
        self.object_name = 'productPackages'
