import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Final
from unittest.mock import MagicMock

import pytest
from pydantic import TypeAdapter

from litellm._internal_context import current_service_target
from litellm.caching.redis_batch import request_redis_batch_scope
from litellm.caching.redis_cache import RedisCache
from litellm.redis.stores.spend_counters import (
    SPEND_COUNTERS_TARGET,
    SpendCounterStore,
    end_user_counter,
    is_unavailable,
    key_counter,
    key_window_counter,
    model_access_group_counter,
    org_counter,
    project_counter,
    shadow_eval_counter,
    tag_counter,
    team_counter,
    team_member_counter,
    team_window_counter,
    user_counter,
)
from litellm.rust_bridge._native import NativeSpendCounterError, NativeSpendCounters
from tests.unit.caching.test_redis_batch import FakeNatives

from .spend_counters_double import FakeSpendCounters

_WIRE: Final = TypeAdapter(dict[str, list[dict[str, object]]]).validate_python(
    json.loads((Path(__file__).parents[3] / "fixtures" / "spend_counters_wire.json").read_text())
)
_BUILDERS: Final[Mapping[tuple[str, bool], Callable[..., str]]] = {
    ("key", False): key_counter,
    ("key", True): key_window_counter,
    ("team", False): team_counter,
    ("team", True): team_window_counter,
    ("team_member", False): team_member_counter,
    ("user", False): user_counter,
    ("end_user", False): end_user_counter,
    ("org", False): org_counter,
    ("tag", False): tag_counter,
    ("project", False): project_counter,
    ("model_access_group", False): model_access_group_counter,
    ("shadow_eval", False): shadow_eval_counter,
}


def _built(case: Mapping[str, object]) -> str:
    ids: Final = TypeAdapter(list[str]).validate_python(case["ids"])
    window: Final = case["window"]
    builder: Final = _BUILDERS[(str(case["scope"]), window is not None)]
    return builder(*ids, window) if window is not None else builder(*ids)


@pytest.mark.parametrize("case", _WIRE["keys"], ids=lambda case: str(case["key"]))
def test_python_builds_every_key_the_native_store_formats(case: Mapping[str, object]) -> None:
    store: Final = NativeSpendCounters(None)

    store.set_local(_built(case), 1.5)

    assert _built(case) == case["key"]
    assert store.snapshot() == {case["key"]: 1.5}


@pytest.mark.parametrize("case", _WIRE["set"], ids=lambda case: str(case["bytes"]))
def test_a_reset_writes_what_redis_cache_set_cache_writes(case: Mapping[str, object]) -> None:
    assert json.dumps(case["spend"]) == case["bytes"]


@pytest.mark.parametrize("case", _WIRE["set_max"], ids=lambda case: str(case["bytes"]))
def test_a_repair_writes_what_redis_cache_set_max_writes(case: Mapping[str, object]) -> None:
    assert str(case["spend"]) == case["bytes"]


@pytest.mark.parametrize("case", _WIRE["read"], ids=lambda case: str(case["bytes"]))
def test_a_read_decodes_what_redis_cache_decodes(case: Mapping[str, object]) -> None:
    decoded: Final = RedisCache._get_cache_logic(  # pyright: ignore[reportPrivateUsage]  # main's codec is the contract
        RedisCache.__new__(RedisCache), str(case["bytes"]).encode()
    )

    assert decoded == case["spend"]
    assert type(decoded) is type(case["spend"])


@pytest.mark.parametrize("key", ["spend_db_floor:spend:key:k", "virtual_key_spend:k:gpt:1d", "spend:unknown:k"])
def test_the_native_store_refuses_keys_outside_the_spend_counter_keyspace(key: str) -> None:
    with pytest.raises(ValueError, match="is not a spend counter"):
        NativeSpendCounters(None).get_local(key)


@pytest.mark.asyncio
async def test_without_redis_the_native_store_counts_and_answers_from_memory() -> None:
    store: Final = SpendCounterStore(None)

    first: Final = await store.increment("spend:key:k", 2)
    second: Final = await store.increment_many((("spend:key:k", 0.5), ("spend:team:t", 1.0)))
    seeded: Final = await store.seed("spend:org:o", 4.0, increment_if_lost=False)
    repaired: Final = await store.set_max("spend:key:k", 9.0)

    assert (first, second, seeded, repaired) == (2, (2.5, 1.0), 4.0, None)
    assert await store.read("spend:key:k") == (9.0, False)
    assert await store.read_many(("spend:key:k",)) is None
    assert not store.has_redis
    assert not store.increment_many_post_call((("spend:key:k", 1.0),))


def test_a_broadcast_writes_and_drops_only_spend_counter_copies() -> None:
    store: Final = SpendCounterStore(None)
    store.set_local("spend:key:k", 5.0)
    store.set_local("spend:team:t", 5.0)

    store.apply_broadcast("spend:key:k", 0.0, 60)
    store.apply_broadcast("spend:team:t", None, None)
    store.apply_broadcast("spend_db_floor:spend:key:k", 1.0, 5)
    store.apply_broadcast("spend:unknown:x", 1.0, None)

    assert store.snapshot() == {"spend:key:k": 0.0}


@pytest.mark.parametrize(
    "kind, dropped",
    [("timeout", True), ("breaker_open", True), ("transport", False), ("rejected", False), ("memory", False)],
)
def test_only_a_timeout_or_an_open_breaker_lets_a_post_call_increment_drop(kind: str, dropped: bool) -> None:
    assert is_unavailable(NativeSpendCounterError(kind, "message")) is dropped
    assert not is_unavailable(TimeoutError(kind))


def test_redis_attaches_once_and_its_ttl_is_the_lease_the_store_reports() -> None:
    built: Final[list[RedisCache | None]] = []

    def handle(connection: RedisCache | None) -> FakeSpendCounters:
        built.append(connection)
        return FakeSpendCounters(redis=None if connection is None else {})

    store: Final = SpendCounterStore(None, handle=handle)
    first: Final = MagicMock(spec=RedisCache, default_ttl=30, namespace=None)
    second: Final = MagicMock(spec=RedisCache, default_ttl=90, namespace=None)

    store.attach_redis(None)
    store.attach_redis(first)
    store.attach_redis(second)

    assert built == [None, first]
    assert store.has_redis
    assert store.redis_ttl == 30


@pytest.mark.asyncio
async def test_post_call_increments_go_on_the_requests_native_post_call_batch_under_the_spend_counters_target() -> None:
    natives: Final = FakeNatives()
    targets: Final[list[str | None]] = []

    class Recording(FakeSpendCounters):
        def increment_many_post_call(self, request: object, increments: object) -> bool:
            targets.append(current_service_target())
            return super().increment_many_post_call(request, increments)  # pyright: ignore[reportArgumentType]  # recorded as given

    spend: Final = Recording()
    store: Final = spend.store()

    outside: Final = store.increment_many_post_call((("spend:key:k", 1.0),))
    with request_redis_batch_scope(native_factory=natives):  # pyright: ignore[reportArgumentType]  # test double
        inside: Final = store.increment_many_post_call((("spend:key:k", 1.0),))
        empty: Final = store.increment_many_post_call(())

    assert (outside, inside, empty) == (False, True, False)
    assert spend.post_call == [(natives.built[0], (("spend:key:k", 1.0),))]
    assert targets == [SPEND_COUNTERS_TARGET]


@pytest.mark.asyncio
async def test_an_unreachable_redis_fails_writes_as_transport_and_serves_reads_from_memory() -> None:
    store: Final = NativeSpendCounters({"host": "127.0.0.1", "port": 1, "socket_timeout": 0.2})
    store.set_local("spend:key:k", 4.0)

    with pytest.raises(NativeSpendCounterError) as failure:
        await store.increment("spend:key:k", 1.0)

    assert failure.value.args[0] == "transport"
    assert not is_unavailable(failure.value)
    assert await store.read("spend:key:k") == ("memory", None)
