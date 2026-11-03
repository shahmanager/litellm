import datetime
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final
from unittest.mock import MagicMock

import pytest
from pydantic import TypeAdapter

from litellm.caching.redis_batch import request_redis_batch_scope
from litellm.caching.redis_cache import RedisCache
from litellm.proxy.common_utils.user_api_key_cache import is_user_key_cache_key
from litellm.redis.stores.auth_cache import AuthCacheSettings, AuthCacheStore
from litellm.rust_bridge._native import NativeAuthCache
from tests.unit.caching.test_redis_batch import FakeNatives

from .auth_cache_double import FakeAuthCache

_WIRE: Final = TypeAdapter(dict[str, list[dict[str, object]]]).validate_python(
    json.loads((Path(__file__).parents[3] / "fixtures" / "auth_cache_wire.json").read_text())
)
_KEY_OBJECT: Final = "88dc28d0f030c55ed4ab77ed8faf098196cb1c05df778539800c9f1243fe6b4b"


@pytest.mark.parametrize("case", _WIRE["keys"], ids=lambda case: str(case["key"]))
def test_the_native_partition_rule_is_is_user_key_cache_key(case: Mapping[str, object]) -> None:
    assert is_user_key_cache_key(str(case["key"])) is case["key_object"]


@pytest.mark.parametrize("case", _WIRE["values"], ids=lambda case: str(case["json"]))
def test_writes_are_what_redis_cache_writes(case: Mapping[str, object]) -> None:
    assert json.dumps(case["value"]) == case["json"]
    assert str(case["value"]) == case["str"]


@pytest.mark.parametrize("case", _WIRE["reads"], ids=lambda case: str(case["bytes"]))
def test_reads_are_what_redis_cache_reads(case: Mapping[str, object]) -> None:
    raw: Final = str(case["bytes"]).encode()

    assert RedisCache._get_cache_logic(MagicMock(spec=RedisCache), raw) == case["value"]  # pyright: ignore[reportPrivateUsage]  # main's own decoder is the contract


@pytest.mark.parametrize(
    "value",
    [
        *(case["value"] for case in _WIRE["values"]),
        ("t1", 2),
        {"members"},
        b"\x00bytes",
        2**80,
        {1: "int key"},
        None,
    ],
)
def test_plain_values_cross_the_bridge_unchanged(value: object) -> None:
    store: Final = NativeAuthCache(None)

    store.set_local("team_id:t1", value)

    assert store.get_local("team_id:t1") == value
    assert type(store.get_local("team_id:t1")) is type(value)


def test_a_live_object_is_refused_rather_than_cached_as_something_else() -> None:
    store: Final = NativeAuthCache(None)

    with pytest.raises(TypeError):
        store.set_local("team_id:t1", {"created_at": datetime.datetime(2026, 1, 1)})

    assert store.get_local("team_id:t1") is None


def test_user_key_objects_live_in_their_own_memory_partition() -> None:
    store: Final = NativeAuthCache(None, memory_entries=2)
    store.set_local(_KEY_OBJECT, {"token": _KEY_OBJECT})

    for index in range(5):
        store.set_local(f"team_id:t{index}", {"team_id": index})

    assert store.get_local(_KEY_OBJECT) == {"token": _KEY_OBJECT}
    assert store.len_local() == (2, 1)


@pytest.mark.asyncio
async def test_a_key_the_request_read_found_absent_is_answered_without_another_read_until_written() -> None:
    fake: Final = FakeAuthCache(redis={"team_id:hit": {"team_id": "hit"}})
    store: Final = AuthCacheStore(MagicMock(spec=RedisCache), AuthCacheSettings(200, 60), handle=fake.factory)

    with request_redis_batch_scope(native_factory=FakeNatives()) as request:  # pyright: ignore[reportArgumentType]  # test double
        found = await store.read_missing(["team_id:absent", "team_id:hit"])
        absent = await store.get("team_id:absent")
        await store.set("team_id:absent", {"team_id": "now"}, None)
        store.delete_local("team_id:absent")
        refilled = await store.get("team_id:absent")
        store.delete_pre_call("team_id:hit")
        deleted = await store.get("team_id:hit")
        await request.flush_all()

    assert found == {"team_id:absent": None, "team_id:hit": {"team_id": "hit"}}
    assert absent is None
    assert refilled == {"team_id": "now"}
    assert deleted is None
    assert [command[:2] for command in fake.commands] == [("SET", "team_id:absent"), ("GET", "team_id:absent")]


@pytest.mark.asyncio
async def test_outside_a_request_a_read_is_a_round_trip_of_its_own() -> None:
    fake: Final = FakeAuthCache(redis={"team_id:t1": {"team_id": "t1"}})
    store: Final = AuthCacheStore(MagicMock(spec=RedisCache), AuthCacheSettings(200, 60), handle=fake.factory)

    assert await store.read_missing(["team_id:t1", "team_id:t2"]) == {
        "team_id:t1": {"team_id": "t1"},
        "team_id:t2": None,
    }
    assert store.set_pre_call("team_id:t3", {}, None) is None
    assert fake.commands == [("MGET", ("team_id:t1", "team_id:t2"))]


def test_only_a_changed_memory_configuration_rebuilds_the_native_store() -> None:
    fake: Final = FakeAuthCache()
    store: Final = AuthCacheStore(MagicMock(spec=RedisCache), AuthCacheSettings(200, 60), handle=fake.factory)

    store.reconfigure(AuthCacheSettings(200, 60))
    store.reconfigure(AuthCacheSettings(500, 5))

    assert fake.settings == [AuthCacheSettings(200, 60), AuthCacheSettings(500, 5)]
    assert store.settings == AuthCacheSettings(500, 5)
