#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Dict, Union
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http.requests_native_auth.abstract_token import AbstractHeaderAuthenticator
from airbyte_cdk.sources.utils.record_helper import stream_data_to_airbyte_message
from airbyte_cdk.models import AirbyteMessage

from source_vetspire_v2.streams import (
    Appointments,
    AppointmentsDeleted,
    AppointmentTypes,
    Clients,
    ConversationsPaginated,
    CreditMemos,
    Encounters,
    EncounterTypes,
    Locations,
    Orders,
    OrderItems,
    Patients,
    PatientPlans,
    PatientProtocols,
    Payments,
    PreventionPlans,
    Products,
    ProductTypes,
    ProductPackages,
    Providers,
    Reservations_DAB011,
    Reservations_DAB012,
    Reservations_DAB010,
    Reservations_DFW012,
    Reservations_DFW010,
    Tasks,
    Vitals,
    IncrementalVetspireV2Stream,
    VetspireV2Stream,
)


## Create authentication method used for Vetspire API
class VetAuth(AbstractHeaderAuthenticator):
    """
    Each token in the cycle is checked against the rate limiter.
    If a token exceeds the capacity limit, the system switches to another token.
    If all tokens are exhausted, the system will enter a sleep state until
    the first token becomes available again.
    """

    DURATION = 3600  # seconds

    def __init__(self, config: Dict, auth_header: str = "Authorization"):
        self._auth_header = auth_header
        self._token = config['api_key']

    @property
    def auth_header(self) -> str:
        return self._auth_header

    @property
    def token(self) -> str:
        return f"{self._token}"


# Define Abstract Source where we m
class SourceVetspireV2(AbstractSource):
    # We must define check_connection even if it is not used
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        return True, None

    # We must define _get_message even if it is not used
    def _get_message(self, record_data_or_message: Union[StreamData, AirbyteMessage], stream: Stream):
        """
        Converts the input to an AirbyteMessage if it is a StreamData. Returns the input as is if it is already an AirbyteMessage
        """
        if isinstance(record_data_or_message, AirbyteMessage):
            return record_data_or_message
        else:
            return stream_data_to_airbyte_message(stream.name, record_data_or_message, stream.transformer, stream.get_json_schema())

    # Define streams with their unique configurations
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = VetAuth(config)
        locations = ["23860", "23771", "23770", "23769", "23768", "23859", "23858", "23857", "23864", "23863", "23069", "23597", "23862",
                     "23861","24069","23889"]
        # test location: "23079"

        # Use this stream if the stream has limit and offset parameters but no location parameters
        stream_kwargs = {
            "start_datetime": config.get("start_datetime", None),
            "limit": config.get("limit", "300"),
            "offset": config.get("offset", "0"),
            "locations": None
        }
        # Use this stream if the stream has limit, offset, and location parameters
        stream_kwargs_with_locations = {
            "start_datetime": config.get("start_datetime", None),
            "limit": config.get("limit", "300"),
            "offset": config.get("offset", "0"),
            "locations": locations
        }
        # Use this stream if the stream has no limit, offset, or location parameters
        stream_kwargs_no_limit = {
            "start_datetime": config.get("start_datetime", None),
            "limit": None,
            "offset": None,
            "locations": None
        }

        # Set DAB010, DAB011, and DAB012
        stream_kwargs_reservations_DAB010 = stream_kwargs.copy()
        stream_kwargs_reservations_DAB010["limit"] = None
        stream_kwargs_reservations_DAB010["locationId"] = 23860

        # Set DFW010 AND DFW012
        stream_kwargs_reservations_DFW010 = stream_kwargs.copy()
        stream_kwargs_reservations_DFW010["limit"] = None
        stream_kwargs_reservations_DFW010["locationId"] = 23597


        return [Appointments(authenticator=auth, **stream_kwargs),
                AppointmentsDeleted(authenticator=auth, **stream_kwargs),
                AppointmentTypes(authenticator=auth, **stream_kwargs_no_limit),
                Clients(authenticator=auth, **stream_kwargs),
                CreditMemos(authenticator=auth, **stream_kwargs_with_locations),
                Encounters(authenticator=auth, **stream_kwargs_with_locations),
                EncounterTypes(authenticator=auth, **stream_kwargs_no_limit),
                Locations(authenticator=auth, **stream_kwargs_no_limit),
                Orders(authenticator=auth, **stream_kwargs_with_locations),
                OrderItems(authenticator=auth, **stream_kwargs_with_locations),
                PatientProtocols(authenticator=auth, **stream_kwargs_with_locations),
                Patients(authenticator=auth, **stream_kwargs),
                PatientPlans(authenticator=auth, **stream_kwargs),
                PreventionPlans(authenticator=auth, **stream_kwargs_no_limit),
                ProductPackages(authenticator=auth, **stream_kwargs_no_limit),
                ProductTypes(authenticator=auth, **stream_kwargs_no_limit),
                Products(authenticator=auth, **stream_kwargs),
                Providers(authenticator=auth, **stream_kwargs_no_limit),
                Reservations_DAB010(authenticator=auth, **stream_kwargs_reservations_DAB010),
                Reservations_DFW010(authenticator=auth, **stream_kwargs_reservations_DFW010),
                Vitals(authenticator=auth, **stream_kwargs),
                Tasks(authenticator=auth, **stream_kwargs)
                ]
