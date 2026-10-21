use kafka_dashboard::{
    db::{
        self, clusters::ClusterRepository, feishu_alerts::FeishuAlertRepository,
        lag_rules::LagRuleRepository,
    },
    domain::{
        cluster::{ClusterInput, DecryptedClusterConfig, SecurityProtocol},
        feishu_alert::FeishuAlertRuleInput,
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

fn alert_rule(group: &str, topic: &str) -> FeishuAlertRuleInput {
    FeishuAlertRuleInput {
        id: None,
        group_id: group.into(),
        topic: topic.into(),
        lag_threshold: 5000,
        growth_seconds: 60,
        enabled: true,
    }
}

// Public reads may expose only a suffix; persistence and internal reads retain the token.
#[tokio::test]
async fn feishu_settings_replace_rules_without_exposing_token() {
    let (pool, clusters, _) = repositories().await;
    let alerts = FeishuAlertRepository::new(pool.clone());
    let cluster = clusters
        .create(&ClusterInput::plaintext("prod", "broker:9092"))
        .await
        .unwrap();
    let initial = alerts.get(&cluster.id).await.unwrap();
    assert!(!initial.access_token_configured);
    assert!(initial.masked_access_token.is_none());
    assert!(initial.rules.is_empty());
    let saved = alerts
        .replace(
            &cluster.id,
            Some("token-secret"),
            false,
            &[
                alert_rule("orders", "events"),
                alert_rule("billing", "events"),
            ],
        )
        .await
        .unwrap();
    assert!(saved.access_token_configured);
    assert_eq!(saved.masked_access_token.as_deref(), Some("••••••••cret"));
    assert_eq!(saved.rules.len(), 2);
    assert_eq!(alerts.get(&cluster.id).await.unwrap(), saved);
    assert!(
        !serde_json::to_string(&saved)
            .unwrap()
            .contains("token-secret")
    );
    assert!(alerts.token(&cluster.id).await.unwrap().as_deref() == Some("token-secret"));
    let stored: Option<String> =
        sqlx::query_scalar("SELECT access_token FROM feishu_alert_settings WHERE cluster_id = ?")
            .bind(&cluster.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(stored.as_deref() == Some("token-secret"));
    let enabled = alerts.enabled_clusters().await.unwrap();
    assert_eq!(enabled.len(), 1);
    assert_eq!(enabled[0].cluster_id, cluster.id);
    assert!(enabled[0].access_token == "token-secret");
    assert_eq!(enabled[0].rules.len(), 2);
    assert!(!format!("{enabled:?}").contains("token-secret"));
    for token in ["x", "短密钥", "a1b2"] {
        let masked = alerts
            .replace(&cluster.id, Some(token), false, &[])
            .await
            .unwrap();
        assert_eq!(masked.masked_access_token.as_deref(), Some("••••••••"));
        assert!(!serde_json::to_string(&masked).unwrap().contains(token));
    }
}

// Omitted/blank tokens preserve credentials, clears retain rules, and stable IDs retain creation time.
#[tokio::test]
async fn feishu_settings_preserve_tokens_and_rule_ids_but_refresh_revision() {
    let (pool, clusters, _) = repositories().await;
    let alerts = FeishuAlertRepository::new(pool.clone());
    let cluster = clusters
        .create(&ClusterInput::plaintext("prod", "broker:9092"))
        .await
        .unwrap();
    let saved = alerts
        .replace(
            &cluster.id,
            Some("token-secret"),
            false,
            &[alert_rule("orders", "events")],
        )
        .await
        .unwrap();
    let created_at: String =
        sqlx::query_scalar("SELECT created_at FROM feishu_alert_rules WHERE id = ?")
            .bind(&saved.rules[0].id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let revision = alerts.enabled_clusters().await.unwrap()[0].revision.clone();
    let mut edited = alert_rule("orders", "events");
    edited.id = Some(saved.rules[0].id.clone());
    edited.lag_threshold = 0;
    edited.growth_seconds = 10;
    for token in [None, Some(""), Some("   ")] {
        let retained = alerts
            .replace(&cluster.id, token, false, &[edited.clone()])
            .await
            .unwrap();
        assert!(retained.access_token_configured);
        assert!(alerts.token(&cluster.id).await.unwrap().as_deref() == Some("token-secret"));
        assert_eq!(retained.rules[0].id, saved.rules[0].id);
        assert_eq!(retained.rules[0].lag_threshold, 0);
        assert_eq!(retained.rules[0].growth_seconds, 10);
    }
    assert_ne!(
        alerts.enabled_clusters().await.unwrap()[0].revision,
        revision
    );
    let retained_created_at: String =
        sqlx::query_scalar("SELECT created_at FROM feishu_alert_rules WHERE id = ?")
            .bind(&saved.rules[0].id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(retained_created_at, created_at);
    let cleared = alerts
        .replace(&cluster.id, None, true, &[edited])
        .await
        .unwrap();
    assert!(!cleared.access_token_configured);
    assert!(cleared.masked_access_token.is_none());
    assert!(alerts.token(&cluster.id).await.unwrap().is_none());
    assert_eq!(cleared.rules.len(), 1);
    assert!(cleared.rules[0].enabled);
    assert!(alerts.enabled_clusters().await.unwrap().is_empty());
}

// A late database error must roll back the token, revision, deleted rules, and new inserts together.
#[tokio::test]
async fn feishu_settings_replace_is_atomic_and_validates_rule_ownership() {
    let (pool, clusters, _) = repositories().await;
    let alerts = FeishuAlertRepository::new(pool.clone());
    let cluster = clusters
        .create(&ClusterInput::plaintext("prod", "broker:9092"))
        .await
        .unwrap();
    let other = clusters
        .create(&ClusterInput::plaintext("other", "broker:9092"))
        .await
        .unwrap();
    let before = alerts
        .replace(
            &cluster.id,
            Some("token-secret"),
            false,
            &[alert_rule("orders", "events")],
        )
        .await
        .unwrap();
    let other_settings = alerts
        .replace(&other.id, None, false, &[alert_rule("orders", "events")])
        .await
        .unwrap();
    let revision = alerts.enabled_clusters().await.unwrap()[0].revision.clone();
    let mut foreign = alert_rule("foreign", "topic");
    foreign.id = Some(other_settings.rules[0].id.clone());
    let mut unknown = alert_rule("unknown", "topic");
    unknown.id = Some("missing-rule".into());
    let mut negative = alert_rule("negative", "topic");
    negative.lag_threshold = -1;
    let mut duration = alert_rule("duration", "topic");
    duration.growth_seconds = 9;
    for invalid in [
        foreign,
        unknown,
        negative,
        duration,
        alert_rule(" ", "topic"),
        alert_rule("group", ""),
    ] {
        assert!(
            alerts
                .replace(&cluster.id, Some("replacement-secret"), false, &[invalid])
                .await
                .is_err()
        );
        assert_eq!(alerts.get(&cluster.id).await.unwrap(), before);
    }
    assert!(
        alerts
            .replace(&cluster.id, Some("replacement-secret"), true, &[])
            .await
            .is_err()
    );
    assert!(
        alerts
            .replace(
                &cluster.id,
                None,
                false,
                &[
                    alert_rule("orders", "events"),
                    alert_rule("orders", "events")
                ]
            )
            .await
            .is_err()
    );
    let mut repeated_id = alert_rule("different", "topic");
    repeated_id.id = Some(before.rules[0].id.clone());
    let mut reused_id = alert_rule("orders", "events");
    reused_id.id = repeated_id.id.clone();
    assert!(
        alerts
            .replace(&cluster.id, None, false, &[reused_id, repeated_id])
            .await
            .is_err()
    );
    sqlx::query("CREATE TRIGGER reject_feishu_rule BEFORE INSERT ON feishu_alert_rules WHEN NEW.group_id = 'reject' BEGIN SELECT RAISE(ABORT, 'fixture failure'); END").execute(&pool).await.unwrap();
    assert!(
        alerts
            .replace(
                &cluster.id,
                Some("replacement-secret"),
                false,
                &[alert_rule("first", "topic"), alert_rule("reject", "topic")]
            )
            .await
            .is_err()
    );
    assert_eq!(alerts.get(&cluster.id).await.unwrap(), before);
    assert!(alerts.token(&cluster.id).await.unwrap().as_deref() == Some("token-secret"));
    assert_eq!(
        alerts.enabled_clusters().await.unwrap()[0].revision,
        revision
    );
    assert!(matches!(
        alerts.replace("missing", None, false, &[]).await,
        Err(db::RepositoryError::ClusterNotFound)
    ));
    assert!(matches!(
        alerts.get("missing").await,
        Err(db::RepositoryError::ClusterNotFound)
    ));
    let replaced = alerts
        .replace(
            &cluster.id,
            None,
            false,
            &[
                alert_rule("Orders", "events"),
                alert_rule("orders", " events "),
            ],
        )
        .await
        .unwrap();
    assert_eq!(replaced.rules.len(), 2);
    assert_eq!(replaced.rules[1].topic, " events ");
    assert!(
        replaced
            .rules
            .iter()
            .all(|rule| rule.id != before.rules[0].id)
    );
    assert!(
        alerts
            .replace(&cluster.id, None, false, &[])
            .await
            .unwrap()
            .rules
            .is_empty()
    );
}

// The worker sees exactly eligible clusters and enabled rules; deletion removes both tables.
#[tokio::test]
async fn feishu_settings_filter_workers_and_cascade_on_cluster_delete() {
    let (pool, clusters, _) = repositories().await;
    let alerts = FeishuAlertRepository::new(pool.clone());
    let mut ids = Vec::new();
    for name in ["enabled", "disabled", "no-token", "no-rules"] {
        ids.push(
            clusters
                .create(&ClusterInput::plaintext(name, "broker:9092"))
                .await
                .unwrap()
                .id,
        );
    }
    let mut disabled = alert_rule("disabled", "events");
    disabled.enabled = false;
    alerts
        .replace(
            &ids[0],
            Some("token-secret"),
            false,
            &[alert_rule("enabled", "events"), disabled.clone()],
        )
        .await
        .unwrap();
    alerts
        .replace(&ids[1], Some("other-secret"), false, &[disabled])
        .await
        .unwrap();
    alerts
        .replace(&ids[2], None, false, &[alert_rule("orders", "events")])
        .await
        .unwrap();
    alerts
        .replace(&ids[3], Some("other-secret"), false, &[])
        .await
        .unwrap();
    let eligible = alerts.enabled_clusters().await.unwrap();
    assert_eq!(eligible.len(), 1);
    assert_eq!(eligible[0].cluster_id, ids[0]);
    assert_eq!(eligible[0].rules.len(), 1);
    assert_eq!(eligible[0].rules[0].group_id, "enabled");
    sqlx::query("UPDATE feishu_alert_settings SET access_token = '   ' WHERE cluster_id = ?")
        .bind(&ids[0])
        .execute(&pool)
        .await
        .unwrap();
    assert!(alerts.enabled_clusters().await.unwrap().is_empty());
    clusters.delete(&ids[0]).await.unwrap();
    for query in [
        "SELECT count(*) FROM feishu_alert_settings WHERE cluster_id = ?",
        "SELECT count(*) FROM feishu_alert_rules WHERE cluster_id = ?",
    ] {
        let remaining: i64 = sqlx::query_scalar(query)
            .bind(&ids[0])
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(remaining, 0);
    }
    assert_eq!(alerts.get(&ids[1]).await.unwrap().rules.len(), 1);
    db::MIGRATOR.run(&pool).await.unwrap();
}

// SQL constraints protect stored rule invariants even when inserts bypass the repository.
#[tokio::test]
async fn feishu_settings_migration_rejects_invalid_rules_and_duplicate_pairs() {
    let (pool, clusters, _) = repositories().await;
    let cluster = clusters
        .create(&ClusterInput::plaintext("prod", "broker:9092"))
        .await
        .unwrap();
    for (threshold, seconds, enabled) in [(-1, 10, 1), (0, 9, 1), (0, 10, 2)] {
        assert!(sqlx::query("INSERT INTO feishu_alert_rules (id, cluster_id, group_id, topic, lag_threshold, growth_seconds, enabled, created_at, updated_at) VALUES ('invalid', ?, 'orders', 'events', ?, ?, ?, 'time', 'time')")
            .bind(&cluster.id).bind(threshold).bind(seconds).bind(enabled).execute(&pool).await.is_err());
    }
    let alerts = FeishuAlertRepository::new(pool.clone());
    alerts
        .replace(&cluster.id, None, false, &[alert_rule("orders", "events")])
        .await
        .unwrap();
    assert!(sqlx::query("INSERT INTO feishu_alert_rules (id, cluster_id, group_id, topic, lag_threshold, growth_seconds, enabled, created_at, updated_at) VALUES ('duplicate', ?, 'orders', 'events', 0, 10, 1, 'time', 'time')")
        .bind(&cluster.id).execute(&pool).await.unwrap_err().as_database_error().unwrap().is_unique_violation());
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
