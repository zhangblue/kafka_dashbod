mod support;

use kafka_dashboard::{crypto::SecretCipher, kafka::config::build_client_properties};
use serde_json::{Value, json};
use support::*;

fn secure_cluster(name: &str) -> Value {
    json!({
        "name": name, "bootstrapServers": "broker:9093", "securityProtocol": "SASL_SSL",
        "saslMechanism": "PLAIN", "username": "reader", "password": "fixture-password",
        "caPem": "fixture-ca", "clientCertificatePem": "fixture-certificate",
        "clientKeyPem": "fixture-key", "clientKeyPassphrase": "fixture-passphrase"
    })
}

fn secure_edit(name: &str) -> Value {
    json!({
        "name": name, "bootstrapServers": "edited:9093", "securityProtocol": "SASL_SSL",
        "saslMechanism": "PLAIN", "username": "reader"
    })
}

async fn saved_secret_json(ctx: &TestApp, id: &str) -> (Vec<u8>, Value) {
    let encrypted: Vec<u8> =
        sqlx::query_scalar("SELECT encrypted_secrets FROM clusters WHERE id = ?")
            .bind(id)
            .fetch_one(&ctx.pool)
            .await
            .unwrap();
    let decrypted = SecretCipher::from_key([7; 32]).decrypt(&encrypted).unwrap();
    (encrypted, serde_json::from_slice(&decrypted).unwrap())
}

// Missing passphrase persistence/mapping would make password-protected TLS keys unusable.
#[tokio::test]
async fn key_passphrase_is_encrypted_preserved_replaced_and_mapped_without_public_echo() {
    let ctx = test_app().await;
    let response = request(
        &ctx.app,
        "POST",
        "/api/clusters",
        Some(secure_cluster("secure")),
    )
    .await;
    assert_eq!(response.status(), 201);
    let created = response_json(response).await;
    let id = created["id"].as_str().unwrap();
    for flag in [
        "passwordConfigured",
        "caPemConfigured",
        "clientCertificatePemConfigured",
        "clientKeyPemConfigured",
        "clientKeyPassphraseConfigured",
    ] {
        assert_eq!(created[flag], true);
    }
    for replacement in [None, Some(""), Some("fixture-new-passphrase")] {
        let mut edit = secure_edit("secure");
        if let Some(value) = replacement {
            edit["clientKeyPassphrase"] = json!(value);
        }
        let response = request(&ctx.app, "PUT", &format!("/api/clusters/{id}"), Some(edit)).await;
        assert_eq!(response.status(), 200);
        assert!(
            response_json(response)
                .await
                .get("clientKeyPassphrase")
                .is_none()
        );
        let expected = if replacement == Some("fixture-new-passphrase") {
            "fixture-new-passphrase"
        } else {
            "fixture-passphrase"
        };
        let (encrypted, secrets) = saved_secret_json(&ctx, id).await;
        assert_eq!(secrets["client_key_passphrase"], expected);
        assert!(
            !encrypted
                .windows(expected.len())
                .any(|part| part == expected.as_bytes())
        );
        let config = ctx.clusters.decrypted_config(id).await.unwrap().unwrap();
        assert_eq!(
            build_client_properties(&config)
                .unwrap()
                .get("ssl.key.password"),
            Some(expected)
        );
        assert!(!format!("{config:?}").contains(expected));
    }
    let public = response_json(request(&ctx.app, "GET", "/api/clusters", None).await).await;
    assert!(public[0].get("clientKeyPassphrase").is_none());
    assert!(!public.to_string().contains("fixture-passphrase"));
}

// Clearing must be independent per field and validate after the final credential merge.
#[tokio::test]
async fn explicit_clear_removes_all_five_secret_fields_and_allows_protocol_switch() {
    let ctx = test_app().await;
    let created = response_json(
        request(
            &ctx.app,
            "POST",
            "/api/clusters",
            Some(secure_cluster("secure")),
        )
        .await,
    )
    .await;
    let id = created["id"].as_str().unwrap();
    let mut clear_ca = secure_edit("secure");
    clear_ca["clearCaPem"] = json!(true);
    assert_eq!(
        request(
            &ctx.app,
            "PUT",
            &format!("/api/clusters/{id}"),
            Some(clear_ca)
        )
        .await
        .status(),
        200
    );
    let (_, secrets) = saved_secret_json(&ctx, id).await;
    assert!(secrets["ca_pem"].is_null());
    assert_eq!(secrets["password"], "fixture-password");
    assert_eq!(secrets["client_key_pem"], "fixture-key");

    let mut clear_passphrase = secure_edit("secure");
    clear_passphrase["clearClientKeyPassphrase"] = json!(true);
    assert_eq!(
        request(
            &ctx.app,
            "PUT",
            &format!("/api/clusters/{id}"),
            Some(clear_passphrase)
        )
        .await
        .status(),
        200
    );
    let (_, secrets) = saved_secret_json(&ctx, id).await;
    assert!(secrets["client_key_passphrase"].is_null());
    assert_eq!(secrets["client_key_pem"], "fixture-key");

    let mut plain = cluster("secure");
    for field in [
        "clearPassword",
        "clearCaPem",
        "clearClientCertificatePem",
        "clearClientKeyPem",
        "clearClientKeyPassphrase",
    ] {
        plain[field] = json!(true);
    }
    let cleared = request(&ctx.app, "PUT", &format!("/api/clusters/{id}"), Some(plain)).await;
    assert_eq!(cleared.status(), 200);
    let cleared = response_json(cleared).await;
    for flag in [
        "passwordConfigured",
        "caPemConfigured",
        "clientCertificatePemConfigured",
        "clientKeyPemConfigured",
        "clientKeyPassphraseConfigured",
    ] {
        assert_eq!(cleared[flag], false);
    }
    let (_, secrets) = saved_secret_json(&ctx, id).await;
    for field in [
        "password",
        "ca_pem",
        "client_certificate_pem",
        "client_key_pem",
        "client_key_passphrase",
    ] {
        assert!(secrets[field].is_null(), "{field} was retained");
    }
    let config = ctx.clusters.decrypted_config(id).await.unwrap().unwrap();
    assert!(build_client_properties(&config).is_ok());
}

// Rejecting only the incoming fields permits an invalid protocol switch or an orphan key.
#[tokio::test]
async fn invalid_merged_security_edits_are_rejected_atomically() {
    let ctx = test_app().await;
    let created = response_json(
        request(
            &ctx.app,
            "POST",
            "/api/clusters",
            Some(secure_cluster("secure")),
        )
        .await,
    )
    .await;
    let id = created["id"].as_str().unwrap();
    let before = saved_secret_json(&ctx, id).await.0;
    let mut cases = vec![cluster("renamed")];
    for field in [
        "clearPassword",
        "clearClientCertificatePem",
        "clearClientKeyPem",
    ] {
        let mut edit = secure_edit("renamed");
        edit[field] = json!(true);
        cases.push(edit);
    }
    for edit in cases {
        let response = request(&ctx.app, "PUT", &format!("/api/clusters/{id}"), Some(edit)).await;
        assert_eq!(response.status(), 422);
        assert_eq!(response_json(response).await["code"], "VALIDATION_ERROR");
        assert_eq!(saved_secret_json(&ctx, id).await.0, before);
        assert_eq!(ctx.clusters.get(id).await.unwrap().unwrap().name, "secure");
    }
}

// Contradictory replace-and-clear intent must never silently erase a supplied credential.
#[tokio::test]
async fn replacing_and_clearing_the_same_secret_is_rejected() {
    let ctx = test_app().await;
    let created = response_json(
        request(
            &ctx.app,
            "POST",
            "/api/clusters",
            Some(secure_cluster("secure")),
        )
        .await,
    )
    .await;
    let id = created["id"].as_str().unwrap();
    for (field, clear) in [
        ("password", "clearPassword"),
        ("caPem", "clearCaPem"),
        ("clientCertificatePem", "clearClientCertificatePem"),
        ("clientKeyPem", "clearClientKeyPem"),
        ("clientKeyPassphrase", "clearClientKeyPassphrase"),
    ] {
        let mut edit = secure_edit("secure");
        edit[field] = json!("fixture-conflict");
        edit[clear] = json!(true);
        let response = request(&ctx.app, "PUT", &format!("/api/clusters/{id}"), Some(edit)).await;
        assert_eq!(response.status(), 422, "conflicting {field}");
        assert!(!response_text(response).await.contains("fixture-conflict"));
    }
}

// Create must obey the same security constraints as draft tests and updates.
#[tokio::test]
async fn invalid_new_security_configurations_never_persist() {
    let ctx = test_app().await;
    let mut cases = vec![];
    let mut orphan = cluster("orphan");
    orphan["securityProtocol"] = json!("SSL");
    orphan["clientKeyPassphrase"] = json!("fixture-orphan");
    cases.push(orphan);
    let mut plaintext = secure_cluster("plain");
    plaintext["securityProtocol"] = json!("PLAINTEXT");
    cases.push(plaintext);
    for input in cases {
        let response = request(&ctx.app, "POST", "/api/clusters", Some(input)).await;
        assert_eq!(response.status(), 422);
    }
    assert!(ctx.clusters.list().await.unwrap().is_empty());
}
