use kafka_dashboard::crypto::SecretCipher;
use kafka_dashboard::{
    config::Config,
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

async fn repositories() -> (sqlx::SqlitePool, ClusterRepository, LagRuleRepository) {
    let pool = db::connect("sqlite::memory:").await.unwrap();
    let clusters = ClusterRepository::new(pool.clone(), SecretCipher::from_key([7; 32]));
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

// Secrets leaking into any public DTO, Debug, or stored plaintext breaks this test.
#[tokio::test]
async fn clusters_round_trip_encrypted_secrets_and_expose_only_safe_dtos() {
    let (pool, clusters, _) = repositories().await;
    let input = cluster_input("fixture-cluster");
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
        "encryptedSecrets",
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
    let stored: Vec<u8> = sqlx::query("SELECT encrypted_secrets FROM clusters WHERE id = ?")
        .bind(&dto.id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    let public_output = format!("{input:?} {decrypted:?} {dto:?} {json}");
    for secret in [
        "fixture-password",
        "fixture-ca",
        "fixture-certificate",
        "fixture-key",
    ] {
        assert!(!public_output.contains(secret));
        assert!(
            !stored
                .windows(secret.len())
                .any(|window| window == secret.as_bytes())
        );
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
    let mut replacement = ClusterInput::plaintext("replacement", "broker:9092");
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
    assert!(
        clusters
            .decrypted_config(&created.id)
            .await
            .unwrap()
            .unwrap()
            .password
            .is_none()
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
fn plaintext_config_has_no_credentials_and_config_requires_a_valid_master_key() {
    let plaintext = DecryptedClusterConfig::plaintext("broker:9092");
    assert_eq!(plaintext.bootstrap_servers, "broker:9092");
    assert_eq!(plaintext.security_protocol, SecurityProtocol::Plaintext);
    assert!(plaintext.password.is_none() && plaintext.client_key_pem.is_none());
    assert_eq!(
        plaintext.connection_timeout,
        std::time::Duration::from_secs(10)
    );
    use base64::{Engine, engine::general_purpose::STANDARD};
    let config = Config::from_values("sqlite::memory:", &STANDARD.encode([7; 32])).unwrap();
    assert_eq!(config.database_url, "sqlite::memory:");
    assert!(Config::from_values("sqlite::memory:", "fixture-invalid-key").is_err());
    assert!(!format!("{config:?}").contains(&STANDARD.encode([7; 32])));
}

// A fixed nonce would make repeated plaintext encrypt identically.
#[test]
fn ciphertext_round_trips_and_uses_unique_nonce() {
    let cipher = SecretCipher::from_key([7_u8; 32]);
    let first = cipher.encrypt(b"fixture-secret").unwrap();
    let second = cipher.encrypt(b"fixture-secret").unwrap();
    assert_ne!(&first[1..13], &second[1..13]);
    assert_eq!(first[0], 1);
    assert_eq!(first.len(), 1 + 12 + 14 + 16);
    assert_eq!(cipher.decrypt(&first).unwrap(), b"fixture-secret");
}

// Missing framing or authentication checks must never yield plaintext.
#[test]
fn cipher_rejects_unknown_short_tampered_and_wrong_key_ciphertexts() {
    let cipher = SecretCipher::from_key([7_u8; 32]);
    for length in 0..29 {
        assert!(cipher.decrypt(&vec![1; length]).is_err());
    }
    let encrypted = cipher.encrypt(b"fixture-secret").unwrap();
    let mut unknown = encrypted.clone();
    unknown[0] = 2;
    assert!(cipher.decrypt(&unknown).is_err());
    let mut tampered = encrypted.clone();
    *tampered.last_mut().unwrap() ^= 1;
    assert!(cipher.decrypt(&tampered).is_err());
    assert!(SecretCipher::from_key([8; 32]).decrypt(&encrypted).is_err());
    assert!(!format!("{cipher:?}").contains("fixture-secret"));
}

#[test]
fn master_key_requires_exactly_32_base64_decoded_bytes() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let cipher = SecretCipher::from_base64_key(&STANDARD.encode([7_u8; 32])).unwrap();
    assert!(cipher.decrypt(&cipher.encrypt(b"fixture").unwrap()).is_ok());
    for invalid in [
        "fixture-invalid-base64".to_string(),
        STANDARD.encode([7_u8; 31]),
        STANDARD.encode([7_u8; 33]),
    ] {
        let error = SecretCipher::from_base64_key(&invalid).unwrap_err();
        assert!(!format!("{error:?} {error}").contains(&invalid));
    }
}
