#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from datetime import datetime, timedelta, timezone
from typing import List, Optional
from unittest import TestCase

import freezegun
import pytest
from airbyte_cdk.models import AirbyteMessage, SyncMode, Type
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import read
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest
from airbyte_cdk.test.mock_http.response_builder import (
    FieldPath,
    HttpResponseBuilder,
    RecordBuilder,
    create_record_builder,
    create_response_builder,
)
from airbyte_protocol.models import AirbyteStreamStatus
from unit_tests.sources.integration.integration_source_fixture import SourceFixture

_NOW = datetime.now(timezone.utc)


class RequestBuilder:
    @classmethod
    def legacies_endpoint(cls) -> "RequestBuilder":
        return cls("legacies")

    @classmethod
    def planets_endpoint(cls) -> "RequestBuilder":
        return cls("planets")

    @classmethod
    def users_endpoint(cls) -> "RequestBuilder":
        return cls("users")

    def __init__(self, resource: str) -> None:
        self._resource = resource
        self._start_date: Optional[datetime] = None
        self._end_date: Optional[datetime] = None

    def with_start_date(self, start_date: datetime) -> "RequestBuilder":
        self._start_date = start_date
        return self

    def with_end_date(self, end_date: datetime) -> "RequestBuilder":
        self._end_date = end_date
        return self

    def build(self) -> HttpRequest:
        query_params = {}
        if self._start_date:
            query_params["start_date"] = self._start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        if self._end_date:
            query_params["end_date"] = self._end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        return HttpRequest(
            url=f"https://api.airbyte-test.com/v1/{self._resource}",
            query_params=query_params,
        )


def _create_catalog(stream_name: str, sync_mode: SyncMode = SyncMode.full_refresh):
    return CatalogBuilder().with_stream(name=stream_name, sync_mode=sync_mode).build()


def _create_legacies_request() -> RequestBuilder:
    return RequestBuilder.legacies_endpoint()


def _create_planets_request() -> RequestBuilder:
    return RequestBuilder.planets_endpoint()


def _create_users_request() -> RequestBuilder:
    return RequestBuilder.users_endpoint()


RESPONSE_TEMPLATE = {
    "object": "list",
    "has_more": False,
    "data": [
        {
            "id": "123",
            "created_at": "2024-01-01T07:04:28.000Z"
        }
    ]
}

USER_TEMPLATE = {
    "object": "list",
    "has_more": False,
    "data": [
        {
            "id": "123",
            "created_at": "2024-01-01T07:04:28.000Z",
            "first_name": "Paul",
            "last_name": "Atreides",
        }
    ]
}

PLANET_TEMPLATE = {
    "object": "list",
    "has_more": False,
    "data": [
        {
            "id": "456",
            "created_at": "2024-01-01T07:04:28.000Z",
            "name": "Giedi Prime",
        }
    ]
}

LEGACY_TEMPLATE = {
    "object": "list",
    "has_more": False,
    "data": [
        {
            "id": "l3g4cy",
            "created_at": "2024-02-01T07:04:28.000Z",
            "quote": "What do you leave behind?",
        }
    ]
}


RESOURCE_TO_TEMPLATE = {
    "legacies": LEGACY_TEMPLATE,
    "planets": PLANET_TEMPLATE,
    "users": USER_TEMPLATE,
}


def _create_response() -> HttpResponseBuilder:
    return create_response_builder(
        response_template=RESPONSE_TEMPLATE,
        records_path=FieldPath("data"),
        # pagination_strategy=StripePaginationStrategy()
    )


def _create_record(resource: str) -> RecordBuilder:
    return create_record_builder(
        response_template=RESOURCE_TO_TEMPLATE.get(resource),
        records_path=FieldPath("data"),
        record_id_path=FieldPath("id"),
        record_cursor_path=FieldPath("created_at"),
    )


class FullRefreshStreamTest(TestCase):
    @HttpMocker()
    def test_full_refresh_sync(self, http_mocker):
        start_datetime = _NOW - timedelta(days=14)
        config = {
            "start_date": start_datetime.isoformat()[:-13]+"Z",
        }

        http_mocker.get(
            _create_users_request().build(),
            _create_response().with_record(record=_create_record("users")).with_record(record=_create_record("users")).build(),
        )

        source = SourceFixture()
        actual_messages = read(source, config=config, catalog=_create_catalog("users"))

        assert emits_successful_sync_status_messages(actual_messages.get_stream_statuses("users"))
        assert len(actual_messages.records) == 2
        assert len(actual_messages.state_messages) == 1
        validate_message_order([Type.RECORD, Type.RECORD, Type.STATE], actual_messages.records_and_state_messages)
        assert actual_messages.state_messages[0].state.stream.stream_descriptor.name == "users"
        assert actual_messages.state_messages[0].state.stream.stream_state == {"sync_mode": "full_refresh"}


@freezegun.freeze_time(_NOW)
class IncrementalStreamTest(TestCase):
    @HttpMocker()
    def test_incremental_sync(self, http_mocker):
        start_datetime = _NOW - timedelta(days=14)
        config = {
            "start_date": start_datetime.isoformat()[:-13] + "Z",
        }

        last_record_date_0 = (start_datetime + timedelta(days=4)).isoformat()[:-13] + "Z"
        http_mocker.get(
            _create_planets_request().with_start_date(start_datetime).with_end_date(start_datetime + timedelta(days=7)).build(),
            _create_response().with_record(record=_create_record("planets").with_cursor(last_record_date_0)).with_record(record=_create_record("planets").with_cursor(last_record_date_0)).with_record(record=_create_record("planets").with_cursor(last_record_date_0)).build(),
        )

        last_record_date_1 = (_NOW - timedelta(days=1)).isoformat()[:-13] + "Z"
        http_mocker.get(
            _create_planets_request().with_start_date(start_datetime + timedelta(days=7)).with_end_date(_NOW).build(),
            _create_response().with_record(record=_create_record("planets").with_cursor(last_record_date_1)).with_record(record=_create_record("planets").with_cursor(last_record_date_1)).build(),
        )

        source = SourceFixture()
        actual_messages = read(source, config=config, catalog=_create_catalog(stream_name="planets", sync_mode=SyncMode.incremental))

        assert emits_successful_sync_status_messages(actual_messages.get_stream_statuses("planets"))
        assert len(actual_messages.records) == 5
        assert len(actual_messages.state_messages) == 2
        validate_message_order([Type.RECORD, Type.RECORD, Type.RECORD, Type.STATE, Type.RECORD, Type.RECORD, Type.STATE], actual_messages.records_and_state_messages)
        assert actual_messages.state_messages[0].state.stream.stream_descriptor.name == "planets"
        assert actual_messages.state_messages[0].state.stream.stream_state == {"created_at": (start_datetime + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")}
        assert actual_messages.state_messages[1].state.stream.stream_descriptor.name == "planets"
        assert actual_messages.state_messages[1].state.stream.stream_state == {"created_at": _NOW.strftime("%Y-%m-%dT%H:%M:%SZ")}

    @HttpMocker()
    def test_incremental_running_as_full_refresh(self, http_mocker):
        start_datetime = _NOW - timedelta(days=14)
        config = {
            "start_date": start_datetime.isoformat()[:-13] + "Z",
        }

        last_record_date_0 = (start_datetime + timedelta(days=4)).isoformat()[:-13] + "Z"
        http_mocker.get(
            _create_planets_request().with_start_date(start_datetime).with_end_date(start_datetime + timedelta(days=7)).build(),
            _create_response().with_record(record=_create_record("planets").with_cursor(last_record_date_0)).with_record(record=_create_record("planets").with_cursor(last_record_date_0)).with_record(record=_create_record("planets").with_cursor(last_record_date_0)).build(),
        )

        last_record_date_1 = (_NOW - timedelta(days=1)).isoformat()[:-13] + "Z"
        http_mocker.get(
            _create_planets_request().with_start_date(start_datetime + timedelta(days=7)).with_end_date(_NOW).build(),
            _create_response().with_record(record=_create_record("planets").with_cursor(last_record_date_1)).with_record(record=_create_record("planets").with_cursor(last_record_date_1)).build(),
        )

        source = SourceFixture()
        actual_messages = read(source, config=config, catalog=_create_catalog(stream_name="planets", sync_mode=SyncMode.full_refresh))

        assert emits_successful_sync_status_messages(actual_messages.get_stream_statuses("planets"))
        assert len(actual_messages.records) == 5
        assert len(actual_messages.state_messages) == 1
        validate_message_order([Type.RECORD, Type.RECORD, Type.RECORD, Type.RECORD, Type.RECORD, Type.STATE], actual_messages.records_and_state_messages)
        assert actual_messages.state_messages[0].state.stream.stream_descriptor.name == "planets"
        assert actual_messages.state_messages[0].state.stream.stream_state == {"created_at": _NOW.strftime("%Y-%m-%dT%H:%M:%SZ")}

    @HttpMocker()
    def test_legacy_incremental_sync(self, http_mocker):
        start_datetime = _NOW - timedelta(days=14)
        config = {
            "start_date": start_datetime.isoformat()[:-13] + "Z",
        }

        last_record_date_0 = (start_datetime + timedelta(days=4)).isoformat()[:-13] + "Z"
        http_mocker.get(
            _create_legacies_request().with_start_date(start_datetime).with_end_date(start_datetime + timedelta(days=7)).build(),
            _create_response().with_record(record=_create_record("legacies").with_cursor(last_record_date_0)).with_record(record=_create_record("legacies").with_cursor(last_record_date_0)).with_record(record=_create_record("legacies").with_cursor(last_record_date_0)).build(),
        )

        last_record_date_1 = (_NOW - timedelta(days=1)).isoformat()[:-13] + "Z"
        http_mocker.get(
            _create_legacies_request().with_start_date(start_datetime + timedelta(days=7)).with_end_date(_NOW).build(),
            _create_response().with_record(record=_create_record("legacies").with_cursor(last_record_date_1)).with_record(record=_create_record("legacies").with_cursor(last_record_date_1)).build(),
        )

        source = SourceFixture()
        actual_messages = read(source, config=config, catalog=_create_catalog(stream_name="legacies", sync_mode=SyncMode.incremental))

        assert emits_successful_sync_status_messages(actual_messages.get_stream_statuses("legacies"))
        assert len(actual_messages.records) == 5
        assert len(actual_messages.state_messages) == 2
        validate_message_order([Type.RECORD, Type.RECORD, Type.RECORD, Type.STATE, Type.RECORD, Type.RECORD, Type.STATE], actual_messages.records_and_state_messages)
        assert actual_messages.state_messages[0].state.stream.stream_descriptor.name == "legacies"
        assert actual_messages.state_messages[0].state.stream.stream_state == {"created_at": last_record_date_0}
        assert actual_messages.state_messages[1].state.stream.stream_descriptor.name == "legacies"
        assert actual_messages.state_messages[1].state.stream.stream_state == {"created_at": last_record_date_1}


def emits_successful_sync_status_messages(status_messages: List[AirbyteStreamStatus]) -> bool:
    return (len(status_messages) == 3 and status_messages[0] == AirbyteStreamStatus.STARTED
            and status_messages[1] == AirbyteStreamStatus.RUNNING and status_messages[2] == AirbyteStreamStatus.COMPLETE)


def validate_message_order(expected_message_order: List[Type], messages: List[AirbyteMessage]):
    if len(expected_message_order) != len(messages):
        pytest.fail(f"Expected message order count {len(expected_message_order)} did not match actual messages {len(messages)}")

    for i, message in enumerate(messages):
        if message.type != expected_message_order[i]:
            pytest.fail(f"At index {i} actual message type {message.type.name} did not match expected message type {expected_message_order[i].name}")
