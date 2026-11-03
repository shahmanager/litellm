mod support;

use std::{sync::Arc, time::Duration};

use litellm_redis_client::{Operation, RedisClient, Ttl};
use litellm_redis_store::{
    Number, RequestRedis,
    router_cooldowns::{self, RouterCooldowns},
    spend_counters::{LostSeed, Reading, SpendCounter, SpendCounters},
};
use litellm_redis_testing::{RedisDeployment, Topology};
use rstest::rstest;
use serde_json::Value;
use support::{connect, connect_observed, within};
use tokio::runtime::Handle;

const TTL: Duration = Duration::from_secs(60);
const LONG_DEADLINE: Duration = Duration::from_secs(60);

/// The wire contract `tests/unit/redis/stores/test_spend_counters.py` checks against Python's
/// own key builders and `RedisCache` codec.
fn wire() -> Value {
    serde_json::from_str(include_str!(
        "../../../../tests/fixtures/spend_counters_wire.json"
    ))
    .unwrap()
}

fn number(value: &Value) -> Number {
    value
        .as_i64()
        .map_or_else(|| Number::Float(value.as_f64().unwrap()), Number::Int)
}

fn text(value: &Value) -> String {
    value.as_str().unwrap().to_owned()
}

fn counter(case: &Value) -> SpendCounter {
    let ids = case["ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(text)
        .collect::<Vec<_>>();
    let window = case["window"].as_str().map(str::to_owned);
    let id = ids[0].clone();
    match case["scope"].as_str().unwrap() {
        "key" => SpendCounter::Key { token: id, window },
        "team" => SpendCounter::Team {
            team_id: id,
            window,
        },
        "team_member" => SpendCounter::TeamMember {
            user_id: id,
            team_id: ids[1].clone(),
        },
        "user" => SpendCounter::User(id),
        "end_user" => SpendCounter::EndUser(id),
        "org" => SpendCounter::Org(id),
        "tag" => SpendCounter::Tag(id),
        "project" => SpendCounter::Project(id),
        "model_access_group" => SpendCounter::ModelAccessGroup(id),
        "shadow_eval" => SpendCounter::ShadowEval(id),
        scope => panic!("unknown scope {scope}"),
    }
}

fn team(id: &str) -> SpendCounter {
    SpendCounter::Team {
        team_id: id.to_owned(),
        window: None,
    }
}

fn store(client: Option<RedisClient>) -> Arc<SpendCounters> {
    Arc::new(SpendCounters::new(client, TTL))
}

#[test]
fn every_counter_formats_and_parses_back_to_mains_key() {
    for case in wire()["keys"].as_array().unwrap() {
        let key = text(&case["key"]);

        assert_eq!(counter(case).key(), key);
        assert_eq!(SpendCounter::parse(&key).unwrap(), counter(case), "{key}");
    }
}

#[rstest]
#[case("spend_db_floor:spend:key:abc")]
#[case("virtual_key_spend:abc:gpt:1d")]
#[case("spend:unknown:abc")]
#[case("spend:team_member:no-team")]
#[case("spend:")]
fn keys_outside_the_spend_counter_keyspace_are_refused(#[case] key: &str) {
    assert!(SpendCounter::parse(key).is_err(), "{key}");
}

#[rstest]
#[tokio::test]
async fn reads_values_python_wrote_and_a_clean_miss_is_a_redis_answer(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let store = store(Some(connect(&deployment)));
    let cases = wire()["read"].as_array().unwrap().clone();
    let counters = (0..cases.len())
        .map(|index| team(&format!("t{index}")))
        .collect::<Vec<_>>();
    for (case, counter) in cases.iter().zip(&counters) {
        raw.set(&counter.key(), text(&case["bytes"]), TTL)
            .await
            .unwrap();
    }
    let expected = cases
        .iter()
        .map(|case| Some(number(&case["spend"])))
        .collect::<Vec<_>>();

    for (counter, value) in counters.iter().zip(&expected) {
        assert_eq!(store.read(counter).await.unwrap(), Reading::Redis(*value));
    }
    assert_eq!(store.read_many(&counters).await, Some(expected));
    assert_eq!(
        store.read(&team("absent")).await.unwrap(),
        Reading::Redis(None)
    );
}

#[rstest]
#[tokio::test]
async fn resets_and_repairs_write_mains_bytes_with_the_redis_ttl(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let store = store(Some(connect(&deployment)));
    let wire = wire();
    let counter = team("t");

    for case in wire["set"].as_array().unwrap() {
        store
            .set(&counter, number(&case["spend"]), None)
            .await
            .unwrap();
        assert_eq!(
            raw.get::<String>(&counter.key()).await.unwrap(),
            Some(text(&case["bytes"]))
        );
    }
    for case in wire["set_max"].as_array().unwrap() {
        raw.del(&[&counter.key()]).await.unwrap();
        let repaired = store
            .set_max(&counter, number(&case["spend"]))
            .await
            .unwrap();
        assert_eq!(
            raw.get::<String>(&counter.key()).await.unwrap(),
            Some(text(&case["bytes"]))
        );
        assert_eq!(
            repaired,
            Some(Number::Float(number(&case["spend"]).as_f64()))
        );
    }
    store
        .set(&counter, Number::Int(5), Some(Duration::from_secs(30)))
        .await
        .unwrap();
    let Ttl::Expires(remaining) = raw.ttl(&counter.key()).await.unwrap() else {
        panic!("a reset counter has no TTL");
    };
    assert!(within(remaining, 30), "{remaining:?}");
}

#[rstest]
#[tokio::test]
async fn set_max_only_ever_raises_a_counter(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let store = store(Some(connect(&deployment)));
    let counter = SpendCounter::Key {
        token: "hashed".to_owned(),
        window: None,
    };
    store.increment(&counter, Number::Float(9.5)).await.unwrap();

    let lower = store.set_max(&counter, Number::Float(4.0)).await.unwrap();
    let higher = store.set_max(&counter, Number::Float(12.0)).await.unwrap();

    assert_eq!(lower, Some(Number::Float(9.5)));
    assert_eq!(higher, Some(Number::Float(12.0)));
    assert_eq!(
        raw.get::<String>(&counter.key()).await.unwrap(),
        Some("12.0".to_owned())
    );
    let Ttl::Expires(remaining) = raw.ttl(&counter.key()).await.unwrap() else {
        panic!("a repaired counter has no TTL");
    };
    assert!(within(remaining, 60), "{remaining:?}");
    assert_eq!(
        store.get_local(&counter).unwrap(),
        Some(Number::Float(12.0))
    );
}

#[rstest]
#[tokio::test]
async fn increments_refresh_the_ttl_and_mirror_the_shared_total_into_memory(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let (client, trips) = connect_observed(&deployment);
    let other_pod = store(Some(connect(&deployment)));
    let store = store(Some(client));
    let [first, second] = [team("a"), team("b")];
    other_pod
        .increment(&first, Number::Float(1.25))
        .await
        .unwrap();
    raw.expire(&first.key(), Duration::from_secs(5))
        .await
        .unwrap();

    let totals = store
        .increment_many(&[
            (first.clone(), Number::Float(0.5)),
            (second.clone(), Number::Int(2)),
        ])
        .await
        .unwrap();

    assert_eq!(totals, [Number::Float(1.75), Number::Float(2.0)]);
    assert_eq!(trips.seen(), [Operation::Pipeline { ops: 2 }]);
    let Ttl::Expires(remaining) = raw.ttl(&first.key()).await.unwrap() else {
        panic!("an incremented counter has no TTL");
    };
    assert!(within(remaining, 60), "{remaining:?}");
    assert_eq!(
        raw.get::<String>(&first.key()).await.unwrap(),
        Some("1.75".to_owned())
    );
    assert_eq!(store.get_local(&first).unwrap(), Some(Number::Float(1.75)));
    assert_eq!(store.get_local(&second).unwrap(), Some(Number::Float(2.0)));
}

#[rstest]
#[tokio::test]
async fn a_post_call_increment_from_a_thread_without_a_runtime_lands_in_redis_and_memory(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let store = store(Some(connect(&deployment)));
    let request = RequestRedis::new(Handle::current(), LONG_DEADLINE);
    let counter = team("t");

    let declared = std::thread::spawn({
        let (store, request, counter) = (Arc::clone(&store), request.clone(), counter.clone());
        move || store.increment_many_post_call(&request, &[(counter, Number::Float(3.5))])
    })
    .join()
    .expect("a Python thread has no Tokio context, and declaring there must not panic");
    request.flush_post().await;

    assert!(declared);
    let mirrored =
        litellm_redis_testing::eventually(async || store.get_local(&counter).unwrap()).await;
    assert_eq!(mirrored, Number::Float(3.5));
    assert_eq!(
        raw.get::<String>(&counter.key()).await.unwrap(),
        Some("3.5".to_owned())
    );
}

#[rstest]
#[tokio::test]
async fn post_call_increments_wait_for_the_post_batch_then_land_in_memory(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let (client, trips) = connect_observed(&deployment);
    let store = store(Some(client));
    let request = RequestRedis::new(Handle::current(), LONG_DEADLINE);
    let counter = team("t");

    assert!(store.increment_many_post_call(&request, &[(counter.clone(), Number::Float(3.5))]));
    request.flush_pre().await;

    assert!(trips.seen().is_empty());
    assert_eq!(raw.get::<String>(&counter.key()).await.unwrap(), None);

    request.flush_post().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(trips.seen(), [Operation::Pipeline { ops: 1 }]);
    assert_eq!(
        raw.get::<String>(&counter.key()).await.unwrap(),
        Some("3.5".to_owned())
    );
    assert_eq!(store.get_local(&counter).unwrap(), Some(Number::Float(3.5)));
}

#[rstest]
#[tokio::test]
async fn the_budget_read_rides_the_same_pre_call_pipeline_as_the_cooldown_read(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let (client, trips) = connect_observed(&deployment);
    let cooldowns = RouterCooldowns::new(Some(client.clone()), router_cooldowns::POLICY);
    let spend = store(Some(client));
    let request = RequestRedis::new(Handle::current(), LONG_DEADLINE);
    let counters = [
        SpendCounter::Key {
            token: "hashed".to_owned(),
            window: None,
        },
        team("t"),
    ];
    raw.set(&counters[0].key(), "4.5", TTL).await.unwrap();

    let active = cooldowns
        .declare_active(&request, &["dep-a".to_owned()])
        .unwrap();
    let budget = spend
        .declare_read_many_pre_call(&request, &counters)
        .unwrap();
    request.flush_pre().await;

    assert_eq!(trips.seen(), [Operation::Pipeline { ops: 3 }]);
    assert_eq!(cooldowns.resolve_active(active).await.unwrap(), vec![]);
    assert_eq!(
        budget.resolve().await,
        Some(vec![Some(Number::Float(4.5)), None])
    );
    assert_eq!(trips.seen().len(), 1);
}

#[rstest]
#[tokio::test]
async fn a_seed_lets_the_first_pod_win_and_others_answer_with_its_value(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let first = store(Some(connect(&deployment)));
    let second = store(Some(connect(&deployment)));
    let counter = SpendCounter::Org("org".to_owned());

    let won = first
        .seed(&counter, Number::Float(10.0), LostSeed::Answer)
        .await
        .unwrap();
    let lost = second
        .seed(&counter, Number::Float(99.0), LostSeed::Increment)
        .await
        .unwrap();

    assert_eq!([won, lost], [Number::Float(10.0), Number::Float(10.0)]);
    assert_eq!(
        raw.get::<String>(&counter.key()).await.unwrap(),
        Some("10.0".to_owned())
    );
    let Ttl::Expires(remaining) = raw.ttl(&counter.key()).await.unwrap() else {
        panic!("a seeded counter has no TTL");
    };
    assert!(within(remaining, 60), "{remaining:?}");
    assert_eq!(
        second.get_local(&counter).unwrap(),
        Some(Number::Float(10.0))
    );
}

#[rstest]
#[tokio::test]
async fn delete_drops_memory_and_redis_and_refresh_ttl_reports_whether_redis_held_it(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let store = store(Some(connect(&deployment)));
    let counters = [team("a"), team("b")];
    for counter in &counters {
        store.increment(counter, Number::Int(1)).await.unwrap();
    }
    raw.expire(&counters[0].key(), Duration::from_secs(5))
        .await
        .unwrap();

    assert!(store.refresh_ttl(&counters[0]).await.unwrap());
    let Ttl::Expires(remaining) = raw.ttl(&counters[0].key()).await.unwrap() else {
        panic!("a refreshed counter has no TTL");
    };
    assert!(within(remaining, 60), "{remaining:?}");

    store.delete(&counters).await.unwrap();

    for counter in &counters {
        assert_eq!(raw.get::<String>(&counter.key()).await.unwrap(), None);
        assert_eq!(store.get_local(counter).unwrap(), None);
    }
    assert!(!store.refresh_ttl(&counters[0]).await.unwrap());
}

#[rstest]
#[tokio::test]
async fn a_redis_outage_reads_memory_and_a_failed_increment_drops_the_counter() {
    let deployment = RedisDeployment::start(Topology::Standalone).await.unwrap();
    let store = store(Some(connect(&deployment)));
    let [kept, failed] = [team("kept"), team("failed")];
    store.increment(&kept, Number::Float(2.0)).await.unwrap();
    store.increment(&failed, Number::Float(3.0)).await.unwrap();
    deployment.stop().await.unwrap();

    let read = store.read(&kept).await.unwrap();
    let increment = store.increment(&failed, Number::Float(1.0)).await;

    assert_eq!(read, Reading::Memory(Some(Number::Float(2.0))));
    assert!(increment.is_err());
    assert_eq!(store.get_local(&failed).unwrap(), None);
    assert_eq!(store.read_many(&[kept]).await, None);
}

#[rstest]
#[tokio::test]
async fn a_failed_post_call_increment_drops_the_counter_from_memory() {
    let deployment = RedisDeployment::start(Topology::Standalone).await.unwrap();
    let store = store(Some(connect(&deployment)));
    let request = RequestRedis::new(Handle::current(), LONG_DEADLINE);
    let counter = team("t");
    store.increment(&counter, Number::Float(1.0)).await.unwrap();
    assert!(store.increment_many_post_call(&request, &[(counter.clone(), Number::Float(1.0))]));
    deployment.stop().await.unwrap();

    request.flush_post().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(store.get_local(&counter).unwrap(), None);
}

#[tokio::test]
async fn without_redis_counters_live_in_memory() {
    let store = store(None);
    let counter = team("t");
    let request = RequestRedis::new(Handle::current(), LONG_DEADLINE);

    let totals = store
        .increment_many(&[
            (counter.clone(), Number::Int(2)),
            (counter.clone(), Number::Float(0.5)),
        ])
        .await
        .unwrap();
    let seeded = store
        .seed(&counter, Number::Float(1.0), LostSeed::Answer)
        .await
        .unwrap();
    let raised = store.set_max(&counter, Number::Float(7.0)).await.unwrap();

    assert_eq!(totals, [Number::Int(2), Number::Float(2.5)]);
    assert_eq!(seeded, Number::Float(2.5));
    assert_eq!(raised, None);
    assert_eq!(
        store.read(&counter).await.unwrap(),
        Reading::Memory(Some(Number::Float(7.0)))
    );
    assert!(!store.increment_many_post_call(&request, &[(counter.clone(), Number::Int(1))]));
    assert!(
        store
            .declare_read_many_pre_call(&request, &[counter])
            .is_none()
    );
}
