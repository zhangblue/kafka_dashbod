use kafka_dashboard::{
    db::{self, clusters::ClusterRepository, lag_rules::LagRuleRepository},
    domain::{
        cluster::{ClusterInput, DecryptedClusterConfig, SecurityProtocol},
        lag_rule::LagRuleInput,
    },
};
use sqlx::Row;

fn cluster_input(name: &str) -> ClusterInput {
    let mut input = ClusterInput::plaintext(name, "localhost:9092");
    input.security_protocol = SecurityProtocol::SaslSsl;
    input.sasl_mechanism = Some("SCRAM-SHA-256".into());
    input.username = Some("fixture-user".into());
    input.password = Some("fixture-password".into());
    input.ca_pem = Some("fixture-ca".into());
    input.client_certificate_pem = Some("fixture-certificate".into());
    input.client_key_pem = Some("fixture-key".into());
    input
}

fn cluster_edit(name: &str) -> ClusterInput {
    let mut input = ClusterInput::plaintext(name, "broker:9092");
    input.security_protocol = SecurityProtocol::SaslSsl;
    input.sasl_mechanism = Some("SCRAM-SHA-256".into());
    input.username = Some("fixture-user".into());
    input
}

async fn repositories() -> (sqlx::SqlitePool, ClusterRepository, LagRuleRepository) {
    let pool = db::connect("sqlite::memory:").await.unwrap();
    let clusters = ClusterRepository::new(pool.clone());
    let rules = LagRuleRepository::new(pool.clone());
    (pool, clusters, rules)
}

fn rule(group: &str, topic: &str) -> LagRuleInput {
    LagRuleInput {
        group_id: group.into(),
        topic: topic.into(),
        normal_lag_max: 10,
        high_lag_min: 100,
        over_limit_seconds: 5,
    }
}

// Missing plaintext persistence or secrets leaking into public output breaks this contract.
#[tokio::test]
async fn clusters_store_plaintext_secret_json_but_public_reads_only_expose_flags() {
    let (pool, clusters, _) = repositories().await;
    let mut input = cluster_input("fixture-cluster");
    input.client_key_passphrase = Some("fixture-passphrase".into());
    let created = clusters.create(&input).await.unwrap();
    let dto = clusters.get(&created.id).await.unwrap().unwrap();
    assert_eq!(dto.name, "fixture-cluster");
    assert_eq!(
        (
            dto.default_normal_lag_max,
            dto.default_high_lag_min,
            dto.default_over_limit_seconds
        ),
        (999, 5000, 30)
    );
    assert_eq!(dto.connection_timeout_ms, 10_000);
    assert_eq!(clusters.list().await.unwrap().len(), 1);
    let json = serde_json::to_value(&dto).unwrap();
    for field in [
        "password",
        "caPem",
        "clientCertificatePem",
        "clientKeyPem",
        "clientKeyPassphrase",
        "secretsJson",
    ] {
        assert!(json.get(field).is_none());
    }
    let decrypted = clusters
        .decrypted_config(&created.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decrypted.password.as_deref(), Some("fixture-password"));
    assert_eq!(decrypted.ca_pem.as_deref(), Some("fixture-ca"));
    assert_eq!(
        decrypted.client_certificate_pem.as_deref(),
        Some("fixture-certificate")
    );
    assert_eq!(decrypted.client_key_pem.as_deref(), Some("fixture-key"));
    assert_eq!(
        decrypted.connection_timeout,
        std::time::Duration::from_secs(10)
    );
    assert_eq!(json["passwordConfigured"], true);
    assert_eq!(json["clientKeyPassphraseConfigured"], true);
    let stored: Vec<u8> = sqlx::query("SELECT secrets_json FROM clusters WHERE id = ?")
        .bind(&dto.id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    let secrets: serde_json::Value = serde_json::from_slice(&stored).unwrap();
    assert_eq!(secrets["password"], "fixture-password");
    assert_eq!(secrets["client_key_passphrase"], "fixture-passphrase");
    let public_output = format!("{input:?} {decrypted:?} {dto:?} {json}");
    for secret in [
        "fixture-password",
        "fixture-ca",
        "fixture-certificate",
        "fixture-key",
        "fixture-passphrase",
    ] {
        assert!(!public_output.contains(secret));
    }
}

#[tokio::test]
async fn cluster_updates_delete_and_duplicate_names_preserve_repository_contract() {
    let (_, clusters, _) = repositories().await;
    let created = clusters
        .create(&cluster_input("fixture-cluster"))
        .await
        .unwrap();
    assert!(
        clusters
            .create(&cluster_input("fixture-cluster"))
            .await
            .is_err()
    );
    let mut replacement = cluster_edit("replacement");
    replacement.default_normal_lag_max = 20;
    replacement.default_high_lag_min = 200;
    clusters
        .update(&created.id, &replacement)
        .await
        .unwrap()
        .unwrap();
    let updated = clusters.get(&created.id).await.unwrap().unwrap();
    assert_eq!(updated.name, "replacement");
    assert_eq!(updated.default_normal_lag_max, 20);
    assert_eq!(updated.created_at, created.created_at);
    assert_eq!(
        clusters
            .decrypted_config(&created.id)
            .await
            .unwrap()
            .unwrap()
            .password
            .as_deref(),
        Some("fixture-password")
    );
    assert!(
        clusters
            .update("missing", &replacement)
            .await
            .unwrap()
            .is_none()
    );
    assert!(clusters.delete(&created.id).await.unwrap());
    assert!(!clusters.delete(&created.id).await.unwrap());
    assert!(clusters.get(&created.id).await.unwrap().is_none());
    assert!(
        clusters
            .decrypted_config(&created.id)
            .await
            .unwrap()
            .is_none()
    );
}

// Omitted/blank edits must retain every credential; nonempty edits replace only their field.
#[tokio::test]
async fn cluster_update_preserves_omitted_and_empty_secrets_and_merges_partial_edits() {
    let (_, clusters, _) = repositories().await;
    let created = clusters
        .create(&cluster_input("fixture-cluster"))
        .await
        .unwrap();
    let mut edit = cluster_edit("fixture-cluster");
    clusters.update(&created.id, &edit).await.unwrap().unwrap();
    let omitted = clusters
        .decrypted_config(&created.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(omitted.password.as_deref(), Some("fixture-password"));
    assert_eq!(omitted.ca_pem.as_deref(), Some("fixture-ca"));
    assert_eq!(
        omitted.client_certificate_pem.as_deref(),
        Some("fixture-certificate")
    );
    assert_eq!(omitted.client_key_pem.as_deref(), Some("fixture-key"));

    edit.password = Some(String::new());
    edit.ca_pem = Some(String::new());
    edit.client_certificate_pem = Some(String::new());
    edit.client_key_pem = Some(String::new());
    clusters.update(&created.id, &edit).await.unwrap().unwrap();
    let empty = clusters
        .decrypted_config(&created.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(empty.password.as_deref(), Some("fixture-password"));
    assert_eq!(empty.ca_pem.as_deref(), Some("fixture-ca"));
    assert_eq!(
        empty.client_certificate_pem.as_deref(),
        Some("fixture-certificate")
    );
    assert_eq!(empty.client_key_pem.as_deref(), Some("fixture-key"));

    edit.password = Some("fixture-new-password".into());
    edit.client_certificate_pem = Some("fixture-new-certificate".into());
    clusters.update(&created.id, &edit).await.unwrap().unwrap();
    let partial = clusters
        .decrypted_config(&created.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(partial.password.as_deref(), Some("fixture-new-password"));
    assert_eq!(partial.ca_pem.as_deref(), Some("fixture-ca"));
    assert_eq!(
        partial.client_certificate_pem.as_deref(),
        Some("fixture-new-certificate")
    );
    assert_eq!(partial.client_key_pem.as_deref(), Some("fixture-key"));

    edit.password = None;
    edit.client_certificate_pem = None;
    edit.ca_pem = Some("fixture-new-ca".into());
    edit.client_key_pem = Some("fixture-new-key".into());
    clusters.update(&created.id, &edit).await.unwrap().unwrap();
    let final_config = clusters
        .decrypted_config(&created.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        final_config.password.as_deref(),
        Some("fixture-new-password")
    );
    assert_eq!(final_config.ca_pem.as_deref(), Some("fixture-new-ca"));
    assert_eq!(
        final_config.client_certificate_pem.as_deref(),
        Some("fixture-new-certificate")
    );
    assert_eq!(
        final_config.client_key_pem.as_deref(),
        Some("fixture-new-key")
    );
}

// A zero duration means immediate eligibility; only negative durations are invalid.
#[tokio::test]
async fn cluster_duration_accepts_zero_and_rejects_negative_values() {
    let (_, clusters, _) = repositories().await;
    let mut input = ClusterInput::plaintext("fixture-zero-duration", "broker:9092");
    input.default_over_limit_seconds = 0;
    let created = clusters.create(&input).await.unwrap();
    assert_eq!(created.default_over_limit_seconds, 0);
    input.default_over_limit_seconds = -1;
    assert!(clusters.update(&created.id, &input).await.is_err());
    assert_eq!(
        clusters
            .get(&created.id)
            .await
            .unwrap()
            .unwrap()
            .default_over_limit_seconds,
        0
    );
    input.name = "fixture-negative-duration".into();
    assert!(clusters.create(&input).await.is_err());
}

#[tokio::test]
async fn lag_rule_duration_accepts_zero_and_rejects_negative_values() {
    let (pool, clusters, rules) = repositories().await;
    let cluster = clusters
        .create(&ClusterInput::plaintext("fixture-duration", "broker:9092"))
        .await
        .unwrap();
    let mut override_rule = rule("billing", "orders");
    override_rule.over_limit_seconds = 0;
    let saved = rules
        .replace(&cluster.id, &[override_rule.clone()])
        .await
        .unwrap();
    assert_eq!(saved[0].over_limit_seconds, 0);
    assert_eq!(
        rules.list(&cluster.id).await.unwrap()[0].over_limit_seconds,
        0
    );
    override_rule.over_limit_seconds = -1;
    assert!(rules.replace(&cluster.id, &[override_rule]).await.is_err());
    assert_eq!(rules.list(&cluster.id).await.unwrap(), saved);
    assert!(sqlx::query("INSERT INTO lag_rules (id, cluster_id, group_id, topic, normal_lag_max, high_lag_min, over_limit_seconds) VALUES ('fixture-negative-duration', ?, 'shipping', 'orders', 10, 100, -1)").bind(&cluster.id).execute(&pool).await.is_err());
}

// Valid rules must survive both validation failures and a SQL error mid-replacement.
#[tokio::test]
async fn lag_rules_replace_atomically_and_enforce_unique_group_topic() {
    let (pool, clusters, rules) = repositories().await;
    let cluster = clusters
        .create(&ClusterInput::plaintext("fixture", "broker:9092"))
        .await
        .unwrap();
    rules
        .replace(&cluster.id, &[rule("billing", "orders")])
        .await
        .unwrap();
    let before = rules.list(&cluster.id).await.unwrap();
    let mut invalid = rule("shipping", "orders");
    invalid.high_lag_min = invalid.normal_lag_max;
    assert!(
        rules
            .replace(&cluster.id, &[rule("other", "topic"), invalid])
            .await
            .is_err()
    );
    assert_eq!(rules.list(&cluster.id).await.unwrap(), before);
    assert!(
        rules
            .replace(
                &cluster.id,
                &[rule("duplicate", "orders"), rule("duplicate", "orders")]
            )
            .await
            .is_err()
    );
    assert_eq!(rules.list(&cluster.id).await.unwrap(), before);
    assert!(sqlx::query("INSERT INTO lag_rules (id, cluster_id, group_id, topic, normal_lag_max, high_lag_min, over_limit_seconds) VALUES ('duplicate', ?, 'billing', 'orders', 10, 100, 5)").bind(&cluster.id).execute(&pool).await.unwrap_err().as_database_error().unwrap().is_unique_violation());
    sqlx::query("CREATE TRIGGER reject_fixture_rule BEFORE INSERT ON lag_rules WHEN NEW.group_id = 'reject' BEGIN SELECT RAISE(ABORT, 'fixture failure'); END").execute(&pool).await.unwrap();
    assert!(
        rules
            .replace(
                &cluster.id,
                &[rule("first", "topic"), rule("reject", "topic")]
            )
            .await
            .is_err()
    );
    assert_eq!(rules.list(&cluster.id).await.unwrap(), before);
    rules
        .replace(
            &cluster.id,
            &[rule("shipping", "orders"), rule("billing", "payments")],
        )
        .await
        .unwrap();
    assert_eq!(rules.list(&cluster.id).await.unwrap().len(), 2);
    rules.replace(&cluster.id, &[]).await.unwrap();
    assert!(rules.list(&cluster.id).await.unwrap().is_empty());
    assert!(
        rules
            .replace("missing-cluster", &[rule("billing", "orders")])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn migration_defaults_and_foreign_key_cascade_are_applied() {
    let (pool, clusters, rules) = repositories().await;
    sqlx::query("INSERT INTO clusters (id,name,bootstrap_servers,security_protocol,created_at,updated_at) VALUES ('defaults','defaults','broker:9092','PLAINTEXT','fixture-time','fixture-time')").execute(&pool).await.unwrap();
    let defaults = clusters.get("defaults").await.unwrap().unwrap();
    assert_eq!(
        (
            defaults.default_normal_lag_max,
            defaults.default_high_lag_min,
            defaults.default_over_limit_seconds
        ),
        (999, 5000, 30)
    );
    rules
        .replace("defaults", &[rule("billing", "orders")])
        .await
        .unwrap();
    clusters.delete("defaults").await.unwrap();
    assert!(rules.list("defaults").await.unwrap().is_empty());
    db::MIGRATOR.run(&pool).await.unwrap();
}

#[test]
fn plaintext_connection_config_has_no_credentials() {
    let plaintext = DecryptedClusterConfig::plaintext("broker:9092");
    assert_eq!(plaintext.bootstrap_servers, "broker:9092");
    assert_eq!(plaintext.security_protocol, SecurityProtocol::Plaintext);
    assert!(plaintext.password.is_none() && plaintext.client_key_pem.is_none());
    assert_eq!(
        plaintext.connection_timeout,
        std::time::Duration::from_secs(10)
    );
}
