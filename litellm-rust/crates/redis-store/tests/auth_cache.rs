mod support;

use std::time::Duration;

use litellm_python_compat::{Value, json::from_json};
use litellm_redis_client::{Operation, RedisClient, Ttl};
use litellm_redis_store::{
    RequestRedis,
    auth_cache::{self, AuthCache, Settings},
    router_cooldowns::{self, RouterCooldowns},
};
use litellm_redis_testing::{RedisDeployment, Topology};
use rstest::rstest;
use support::{connect, connect_observed, within};
use tokio::runtime::Handle;

const LONG_DEADLINE: Duration = Duration::from_secs(60);
const KEY_OBJECT: &str = "88dc28d0f030c55ed4ab77ed8faf098196cb1c05df778539800c9f1243fe6b4b";

/// The wire contract `tests/unit/redis/stores/test_auth_cache.py` checks against Python's own
/// `json.dumps`, `str` and `RedisCache._get_cache_logic`.
fn wire() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../../tests/fixtures/auth_cache_wire.json"
    ))
    .unwrap()
}

fn cases(section: &str) -> Vec<serde_json::Value> {
    wire()[section].as_array().unwrap().clone()
}

fn text(value: &serde_json::Value) -> String {
    value.as_str().unwrap().to_owned()
}

fn settings(memory_ttl: Option<u64>) -> Settings {
    Settings {
        memory_ttl: memory_ttl.map(Duration::from_secs),
        ..Settings::default()
    }
}

fn store(client: Option<RedisClient>) -> AuthCache {
    AuthCache::new(client, settings(Some(60)))
}

fn team(id: &str) -> Value {
    Value::Dict(vec![(Value::from("team_id"), Value::from(id))])
}

async fn redis_ttl(raw: &RedisClient, key: &str) -> Duration {
    let Ttl::Expires(remaining) = raw.ttl(key).await.unwrap() else {
        panic!("{key} has no TTL");
    };
    remaining
}

#[test]
fn only_a_bare_lowercase_sha256_digest_is_a_user_key_object() {
    for case in cases("keys") {
        assert_eq!(
            auth_cache::is_key_object(&text(&case["key"])),
            case["key_object"].as_bool().unwrap(),
            "{case}"
        );
    }
}

#[rstest]
#[tokio::test]
async fn writes_mains_json_and_str_bytes(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let store = store(Some(connect(&deployment)));

    for (index, case) in cases("values").iter().enumerate() {
        let key = format!("team_id:t{index}");
        store
            .set(&key, from_json(case["value"].clone()), None)
            .await
            .unwrap();
        assert_eq!(
            raw.get::<String>(&key).await.unwrap(),
            Some(text(&case["json"]))
        );
        store
            .set_str(&key, from_json(case["value"].clone()), None)
            .await
            .unwrap();
        assert_eq!(
            raw.get::<String>(&key).await.unwrap(),
            Some(text(&case["str"]))
        );
    }
}

#[rstest]
#[tokio::test]
async fn reads_every_encoding_main_writes_and_backfills_memory(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let store = store(Some(connect(&deployment)));

    for (index, case) in cases("reads").iter().enumerate() {
        let key = format!("user_id:u{index}");
        raw.set(&key, text(&case["bytes"]), Duration::from_secs(60))
            .await
            .unwrap();
        let expected = from_json(case["value"].clone());

        assert_eq!(store.get(&key).await.unwrap(), Some(expected.clone()));
        raw.del(&[&key]).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), Some(expected), "{key}");
    }
    assert_eq!(store.get("user_id:absent").await.unwrap(), None);
}

#[rstest]
#[tokio::test]
async fn ttls_follow_dual_cache_and_the_write_back_rule(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let store = AuthCache::new(
        Some(connect(&deployment)),
        Settings {
            memory_ttl: Some(Duration::from_secs(20)),
            redis_ttl: Duration::from_secs(45),
            ..Settings::default()
        },
    );

    store
        .set(
            "team_id:given",
            team("a"),
            Some(Duration::from_secs_f64(30.9)),
        )
        .await
        .unwrap();
    store.set("team_id:default", team("b"), None).await.unwrap();
    store
        .write_back(
            None,
            vec![
                ("user-1".to_owned(), team("c"), None),
                ("user-2".to_owned(), team("d"), Some(Duration::from_secs(5))),
            ],
        )
        .await
        .unwrap();

    assert!(within(redis_ttl(&raw, "team_id:given").await, 30));
    assert!(within(redis_ttl(&raw, "team_id:default").await, 20));
    assert!(within(redis_ttl(&raw, "user-1").await, 45));
    assert!(within(redis_ttl(&raw, "user-2").await, 5));
    assert_eq!(store.get_local("user-1").unwrap(), Some(team("c")));
}

#[tokio::test]
async fn user_key_objects_survive_churn_in_the_management_object_partition() {
    let store = AuthCache::new(
        None,
        Settings {
            memory_entries: 2,
            ..Settings::default()
        },
    );
    store.set(KEY_OBJECT, team("key"), None).await.unwrap();

    for index in 0..5 {
        store
            .set(&format!("team_id:t{index}"), team("t"), None)
            .await
            .unwrap();
    }

    assert_eq!(store.get(KEY_OBJECT).await.unwrap(), Some(team("key")));
    assert_eq!(store.len_local().unwrap(), (2, 1));
}

#[rstest]
#[tokio::test]
async fn a_value_json_refuses_stays_in_memory_and_never_reaches_redis(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let store = store(Some(connect(&deployment)));
    let members = Value::Set(vec![Value::from("a")]);

    store
        .set("team_id:set", members.clone(), None)
        .await
        .unwrap();

    assert_eq!(store.get_local("team_id:set").unwrap(), Some(members));
    assert_eq!(raw.get::<String>("team_id:set").await.unwrap(), None);
}

#[rstest]
#[tokio::test]
async fn pre_call_writes_and_reads_ride_one_pipeline_with_the_cooldown_read(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let (client, trips) = connect_observed(&deployment);
    let cooldowns = RouterCooldowns::new(Some(client.clone()), router_cooldowns::POLICY);
    let store = store(Some(client));
    let request = RequestRedis::new(Handle::current(), LONG_DEADLINE);
    raw.set(KEY_OBJECT, "{\"token\": \"t\"}", Duration::from_secs(60))
        .await
        .unwrap();
    raw.set("team_alias:alpha", "\"stale\"", Duration::from_secs(60))
        .await
        .unwrap();

    let reads = store
        .declare_get_many(
            &request,
            &[KEY_OBJECT.to_owned(), "end_user_id:absent".to_owned()],
        )
        .unwrap();
    let active = cooldowns
        .declare_active(&request, &["dep-a".to_owned()])
        .unwrap();
    let written = store
        .set_pre_call(&request, "team_id:t1", team("t1"), None)
        .unwrap()
        .unwrap();
    let deleted = store
        .delete_pre_call(&request, "team_alias:alpha")
        .unwrap()
        .unwrap();

    assert!(trips.seen().is_empty());
    assert_eq!(store.get_local("team_id:t1").unwrap(), Some(team("t1")));
    request.flush_pre().await;

    assert_eq!(trips.seen(), [Operation::Pipeline { ops: 5 }]);
    assert_eq!(
        reads.resolve().await.unwrap(),
        vec![
            Some(Value::Dict(vec![(Value::from("token"), Value::from("t"))])),
            None
        ]
    );
    assert_eq!(cooldowns.resolve_active(active).await.unwrap(), vec![]);
    written.resolve().await.unwrap();
    deleted.resolve().await.unwrap();
    assert_eq!(
        raw.get::<String>("team_id:t1").await.unwrap(),
        Some("{\"team_id\": \"t1\"}".to_owned())
    );
    assert!(within(redis_ttl(&raw, "team_id:t1").await, 60));
    assert_eq!(raw.get::<String>("team_alias:alpha").await.unwrap(), None);
    assert_eq!(trips.seen().len(), 1);
}

#[rstest]
#[tokio::test]
async fn a_write_back_on_the_request_waits_for_the_next_flush(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let (client, trips) = connect_observed(&deployment);
    let store = store(Some(client));
    let request = RequestRedis::new(Handle::current(), LONG_DEADLINE);

    store
        .write_back(
            Some(&request),
            vec![(
                "user-1".to_owned(),
                team("u"),
                Some(Duration::from_secs(42)),
            )],
        )
        .await
        .unwrap();

    assert!(trips.seen().is_empty());
    assert_eq!(store.get_local("user-1").unwrap(), Some(team("u")));
    request.flush_pre().await;
    assert_eq!(trips.seen(), [Operation::Pipeline { ops: 1 }]);
    assert!(within(redis_ttl(&raw, "user-1").await, 42));
}

#[rstest]
#[tokio::test]
async fn an_eviction_broadcast_drops_only_this_pods_copy_and_the_next_read_refills_from_redis(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let this_pod = store(Some(connect(&deployment)));
    let other_pod = store(Some(connect(&deployment)));
    this_pod.set("team_id:t1", team("old"), None).await.unwrap();
    other_pod
        .set("team_id:t1", team("new"), None)
        .await
        .unwrap();

    assert_eq!(this_pod.get("team_id:t1").await.unwrap(), Some(team("old")));
    this_pod.delete_local("team_id:t1").unwrap();

    assert_eq!(this_pod.get("team_id:t1").await.unwrap(), Some(team("new")));
    assert!(raw.get::<String>("team_id:t1").await.unwrap().is_some());
}

#[rstest]
#[tokio::test]
async fn delete_drops_memory_and_redis(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let store = store(Some(connect(&deployment)));
    let keys = [KEY_OBJECT.to_owned(), "team_id:t1".to_owned()];
    store
        .set_many(
            keys.iter().map(|key| (key.clone(), team("t"))).collect(),
            None,
        )
        .await
        .unwrap();
    assert!(raw.get::<String>("team_id:t1").await.unwrap().is_some());

    store.delete(&keys).await.unwrap();

    for key in &keys {
        assert_eq!(store.get_local(key).unwrap(), None);
        assert_eq!(raw.get::<String>(key).await.unwrap(), None);
    }
}

#[rstest]
#[tokio::test]
async fn increment_if_present_only_raises_an_entry_redis_holds(
    #[values(Topology::Standalone, Topology::Cluster)] topology: Topology,
) {
    let deployment = RedisDeployment::start(topology).await.unwrap();
    let raw = connect(&deployment);
    let store = store(Some(connect(&deployment)));

    assert_eq!(
        store
            .increment_if_present("litellm-proxy-budget:spend", 2.5)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        raw.get::<String>("litellm-proxy-budget:spend")
            .await
            .unwrap(),
        None
    );
    store
        .set("litellm-proxy-budget:spend", Value::Float(1.5), None)
        .await
        .unwrap();

    assert_eq!(
        store
            .increment_if_present("litellm-proxy-budget:spend", 2.5)
            .await
            .unwrap(),
        Some(4.0)
    );
}

#[tokio::test]
async fn without_redis_every_operation_is_memory_only() {
    let store = store(None);
    let request = RequestRedis::new(Handle::current(), LONG_DEADLINE);

    store.set("team_id:t1", team("t"), None).await.unwrap();

    assert!(
        store
            .set_pre_call(&request, "team_id:t2", team("t"), None)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .declare_get_many(&request, &["team_id:t1".to_owned()])
            .is_none()
    );
    assert!(store.get_many(&["team_id:t1".to_owned()]).await.is_none());
    assert_eq!(store.increment_if_present("k", 1.0).await.unwrap(), None);
    assert_eq!(store.get("team_id:t1").await.unwrap(), Some(team("t")));
    assert_eq!(store.get_local("team_id:t2").unwrap(), None);
}
