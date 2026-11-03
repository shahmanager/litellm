import binascii
import json
from pathlib import Path
from typing import Final

import pytest
from pydantic import TypeAdapter

from litellm.caching.redis_batch import request_redis_batch_scope
from litellm.caching.redis_cache import RedisCache
from litellm.redis.stores.rate_limits import (
    AtomicCounter,
    CounterIncrement,
    RateLimitScope,
    RateLimitStore,
)
from tests.unit.caching.test_redis_batch import FakeNatives

_WIRE: Final = Path(__file__).parents[4] / "litellm-rust/crates/redis-store/tests/fixtures/rate_limits_wire.json"
_SCOPES: Final = TypeAdapter(list[dict[str, str]])
_VALUES: Final = TypeAdapter(list[dict[str, str | int | float]])


def _wire() -> dict[str, object]:
    return json.loads(_WIRE.read_text())


def _mains_key(key: str, value: str, rate_limit_type: str) -> str:
    """``create_rate_limit_keys`` and the window key as ``main``'s limiter formatted them."""
    return f"{{{key}:{value}}}:{rate_limit_type}"


def _mains_keyslot(key: str) -> int:
    """``keyslot_for_redis_cluster`` from ``main``'s limiter."""
    start: Final = key.find("{")
    if start != -1:
        end: Final = key.find("}", start + 1)
        if end != -1 and end != start + 1:
            key = key[start + 1 : end]
    return binascii.crc_hqx(key.encode("utf-8"), 0) % 16384


def test_the_wire_fixture_keys_are_the_keys_mains_limiter_formats():
    for scope in _SCOPES.validate_python(_wire()["scopes"]):
        for suffix in ("window", "requests", "tokens", "max_parallel_requests"):
            assert scope[suffix] == _mains_key(scope["descriptor_key"], scope["descriptor_value"], suffix)


def test_the_wire_fixture_values_decode_as_mains_redis_cache_reads_them():
    redis_cache: Final = RedisCache.__new__(RedisCache)
    for value in _VALUES.validate_python(_wire()["values"]):
        decoded = redis_cache._get_cache_logic(str(value["raw"]).encode())  # pyright: ignore[reportPrivateUsage]  # main's read codec is the contract
        assert decoded == value["decoded"]
        assert type(decoded) is type(value["decoded"])


@pytest.mark.parametrize("force", [True, False])
def test_slot_groups_follow_mains_hash_tag_slots_only_when_grouping_is_forced(force: bool):
    store: Final = RateLimitStore(None)
    scopes: Final = (
        *(
            RateLimitScope(s["descriptor_key"], s["descriptor_value"])
            for s in _SCOPES.validate_python(_wire()["scopes"])
        ),
        RateLimitScope("api_key", "a"),
        RateLimitScope("team", "t"),
        RateLimitScope("api_key", "a"),
        RateLimitScope("tag", ""),
    )
    slots: Final = [_mains_keyslot(_mains_key(s.descriptor_key, s.descriptor_value, "window")) for s in scopes]
    expected: Final = tuple(
        tuple(index for index, slot in enumerate(slots) if slot == group_slot) for group_slot in dict.fromkeys(slots)
    )

    grouped: Final = store.slot_groups(scopes, force)

    assert grouped == (expected if force else (tuple(range(len(scopes))),))


def test_the_memory_tier_keeps_each_value_type_it_was_given():
    store: Final = RateLimitStore(None)
    scope: Final = RateLimitScope("api_key", "k1")
    store.set_local(scope.window, "1730000000", 60)
    store.set_local(scope.counter("requests"), 3, 60)
    store.set_local(scope.counter("tokens"), 2.5, 60)
    store.set_local(scope.gauge, {"slot-1": 1730000000.5}, 60)

    values: Final = store.local((scope.window, scope.counter("requests"), scope.counter("tokens"), scope.gauge))

    assert values == ("1730000000", 3, 2.5, {"slot-1": 1730000000.5})
    assert [type(value) for value in values] == [str, int, float, dict]
    assert store.local((RateLimitScope("api_key", "k2").counter("requests"),)) == (None,)


@pytest.mark.asyncio
async def test_without_redis_increments_add_in_memory_as_python_numbers_do():
    store: Final = RateLimitStore(None)
    tokens: Final = RateLimitScope("team", "t1").counter("tokens")

    assert await store.increment(CounterIncrement(tokens, 5, 60)) == 5
    await store.increment_many((CounterIncrement(tokens, 2.5, 60), CounterIncrement(tokens, -1, 60)))
    await store.increment_many_post_call((CounterIncrement(tokens, 1, 60),))

    assert store.local((tokens,)) == (7.5,)
    assert not store.has_redis
    assert not store.has_post_call_batch


@pytest.mark.asyncio
async def test_without_redis_nothing_is_declared_on_the_request_and_post_call_writes_are_refused():
    store: Final = RateLimitStore(None)
    counter: Final = RateLimitScope("api_key", "k1").counter("requests")
    natives: Final = FakeNatives()

    with request_redis_batch_scope(native_factory=natives):  # pyright: ignore[reportArgumentType]  # test double
        assert store.declare_sliding_window((counter,), 0, 60) is None
        assert store.declare_check_and_increment((AtomicCounter(counter, 10, 1, 60, 60),)) is None
        assert not await store.token_increment_post_call((CounterIncrement(counter, 1, 60),))
        assert not await store.parallel_release_post_call((counter.scope,), "slot")

    assert natives.built == []
