mod support;

// A missing explicit body limit permits oversized configuration payloads.
#[tokio::test]
async fn oversized_configuration_returns_413_without_echoing_secrets() {
    let ctx = support::test_app().await;
    let response = support::request(
        &ctx.app,
        "POST",
        "/api/clusters",
        Some(serde_json::json!({
            "name": "x".repeat(300_000), "password": "limit-fixture-secret"
        })),
    )
    .await;
    assert_eq!(response.status(), 413);
    let text = support::response_text(response).await;
    assert!(!text.contains("limit-fixture-secret"));
    let error: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(error["code"], "PAYLOAD_TOO_LARGE");
    assert!(error["requestId"].is_string());
}

use axum::{body::Body, http::Request};
use serde_json::json;
use support::*;
use tower::ServiceExt;

fn secure_edit(name: &str) -> serde_json::Value {
    json!({"name":name,"bootstrapServers":"broker:9093","securityProtocol":"SASL_SSL","saslMechanism":"PLAIN","username":"reader"})
}

// Any response built from private connection settings instead of the public DTO leaks secrets.
#[tokio::test]
async fn create_read_list_update_and_delete_cluster_never_expose_secrets() {
    let ctx = test_app().await;
    let mut input = secure_edit("prod");
    input["password"] = json!("fixture-password");
    input["clientKeyPem"] = json!("fixture-private-key");
    input["clientCertificatePem"] = json!("fixture-certificate");
    input["caPem"] = json!("fixture-ca");
    let response = request(&ctx.app, "POST", "/api/clusters", Some(input)).await;
    assert_eq!(response.status(), 201);
    let created = response_json(response).await;
    let id = created["id"].as_str().unwrap();
    for value in [
        created.clone(),
        response_json(request(&ctx.app, "GET", &format!("/api/clusters/{id}"), None).await).await,
        response_json(request(&ctx.app, "GET", "/api/clusters", None).await).await,
        response_json(
            request(
                &ctx.app,
                "PUT",
                &format!("/api/clusters/{id}"),
                Some(secure_edit("renamed")),
            )
            .await,
        )
        .await,
    ] {
        let text = value.to_string();
        for forbidden in [
            "fixture-password",
            "fixture-private-key",
            "fixture-ca",
            "\"password\":",
            "\"clientKeyPem\":",
            "encryptedSecrets",
        ] {
            assert!(
                !text.contains(forbidden),
                "unexpected sensitive field in {text}"
            );
        }
    }
    assert_eq!(created["defaultNormalLagMax"], 999);
    assert_eq!(
        request(&ctx.app, "DELETE", &format!("/api/clusters/{id}"), None)
            .await
            .status(),
        204
    );
    let missing = request(&ctx.app, "GET", &format!("/api/clusters/{id}"), None).await;
    assert_eq!(missing.status(), 404);
    assert_eq!(response_json(missing).await["code"], "CLUSTER_NOT_FOUND");
}

// Blank/omitted credentials must retain their old values; clearing must be explicit and field-scoped.
#[tokio::test]
async fn edits_preserve_secrets_until_explicit_password_clear() {
    let ctx = test_app().await;
    let mut input = secure_edit("prod");
    input["password"] = json!("fixture-password");
    input["clientKeyPem"] = json!("fixture-private-key");
    input["clientCertificatePem"] = json!("fixture-certificate");
    let id =
        response_json(request(&ctx.app, "POST", "/api/clusters", Some(input)).await).await["id"]
            .as_str()
            .unwrap()
            .to_owned();
    for value in [
        secure_edit("prod"),
        json!({"name":"prod","bootstrapServers":"broker:9093","securityProtocol":"SASL_SSL","saslMechanism":"PLAIN","username":"reader","password":"","clientKeyPem":""}),
    ] {
        assert_eq!(
            request(&ctx.app, "PUT", &format!("/api/clusters/{id}"), Some(value))
                .await
                .status(),
            200
        );
        let config = ctx.clusters.decrypted_config(&id).await.unwrap().unwrap();
        assert_eq!(config.password.as_deref(), Some("fixture-password"));
        assert_eq!(
            config.client_key_pem.as_deref(),
            Some("fixture-private-key")
        );
    }
    let mut edit = cluster("prod");
    edit["securityProtocol"] = json!("SSL");
    edit["clearPassword"] = json!(true);
    assert_eq!(
        request(&ctx.app, "PUT", &format!("/api/clusters/{id}"), Some(edit))
            .await
            .status(),
        200
    );
    let config = ctx.clusters.decrypted_config(&id).await.unwrap().unwrap();
    assert!(config.password.is_none());
    assert_eq!(
        config.client_key_pem.as_deref(),
        Some("fixture-private-key")
    );
}

// Validation and storage errors must produce stable codes, useful fields, and no rejected input.
#[tokio::test]
async fn invalid_clusters_duplicate_names_and_missing_ids_return_safe_errors() {
    let ctx = test_app().await;
    for (field, value) in [
        ("name", json!(" ")),
        ("bootstrapServers", json!("")),
        ("connectionTimeoutMs", json!(0)),
        ("defaultNormalLagMax", json!(-1)),
        ("defaultHighLagMin", json!(999)),
        ("defaultOverLimitSeconds", json!(-1)),
    ] {
        let mut input = cluster("invalid");
        input[field] = value;
        let response = request(&ctx.app, "POST", "/api/clusters", Some(input)).await;
        assert_eq!(response.status(), 422, "field {field}");
        let body = response_json(response).await;
        assert_eq!(body["code"], "VALIDATION_ERROR");
        assert!(
            body["details"][field].is_string(),
            "missing field {field}: {body}"
        );
    }
    create_cluster(&ctx.app, "prod").await;
    let duplicate = request(&ctx.app, "POST", "/api/clusters", Some(cluster("prod"))).await;
    assert_eq!(duplicate.status(), 409);
    assert_eq!(response_json(duplicate).await["code"], "CONFLICT");
    for method in ["GET", "PUT", "DELETE"] {
        let response = request(
            &ctx.app,
            method,
            "/api/clusters/missing",
            Some(cluster("new")),
        )
        .await;
        assert_eq!(response.status(), 404);
        assert_eq!(response_json(response).await["code"], "CLUSTER_NOT_FOUND");
    }
}

// Default framework JSON rejection text can echo input; errors must use the shared envelope.
#[tokio::test]
async fn malformed_json_and_unknown_routes_use_request_ids_and_safe_envelopes() {
    let ctx = test_app().await;
    for (uri, body, expected_code, status) in [
        (
            "/api/clusters",
            r#"{"password":"fixture-secret", "securityProtocol":"fixture-secret"}"#,
            "VALIDATION_ERROR",
            422,
        ),
        (
            "/api/clusters",
            r#"{"password":"fixture-secret", "#,
            "VALIDATION_ERROR",
            422,
        ),
        ("/unknown", "{}", "NOT_FOUND", 404),
    ] {
        let response = ctx
            .app
            .clone()
            .oneshot(
                Request::post(uri)
                    .header("content-type", "application/json")
                    .header("x-request-id", "fixture-request-id")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["x-request-id"], "fixture-request-id");
        let text = response_text(response).await;
        assert!(!text.contains("fixture-secret"));
        let body: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(body["code"], expected_code);
        assert_eq!(body["requestId"], "fixture-request-id");
        assert!(body["message"].is_string());
    }
    let response = request(&ctx.app, "GET", "/api/clusters", None).await;
    assert!(uuid::Uuid::parse_str(response.headers()["x-request-id"].to_str().unwrap()).is_ok());
}

// Path decoding is rejected before handlers run; it still needs the public error contract.
#[tokio::test]
async fn invalid_path_encoding_returns_the_shared_json_error_envelope() {
    let ctx = test_app().await;
    let response = request(&ctx.app, "GET", "/api/clusters/%FF", None).await;
    assert_eq!(response.status(), 400);
    let id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let text = response_text(response).await;
    assert!(text.starts_with('{'), "expected JSON envelope, got {text}");
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    assert_eq!(body["requestId"], id);
    assert!(body["message"].is_string());
}
