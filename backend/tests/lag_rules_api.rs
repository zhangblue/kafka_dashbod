mod support;

use serde_json::{Value, json};
use support::*;

fn rules() -> Value {
    json!({"defaults":{"normalMax":12,"highMin":50,"overLimitSeconds":0},
        "overrides":[{"groupId":"billing","topic":"orders","normalMax":4,"highMin":20,"overLimitSeconds":0}]})
}

// PUT must update defaults as well as replace, rather than append, the full override list.
#[tokio::test]
async fn get_and_put_lag_rules_update_defaults_and_replace_all_overrides() {
    let ctx = test_app().await;
    let id = create_cluster(&ctx.app, "prod").await;
    let uri = format!("/api/clusters/{id}/lag-rules");
    let initial = response_json(request(&ctx.app, "GET", &uri, None).await).await;
    assert_eq!(
        initial,
        json!({"defaults":{"normalMax":999,"highMin":5000,"overLimitSeconds":30},"overrides":[]})
    );
    let response = request(&ctx.app, "PUT", &uri, Some(rules())).await;
    assert_eq!(response.status(), 200);
    let updated = response_json(response).await;
    assert_eq!(updated["defaults"], rules()["defaults"]);
    assert_eq!(updated["overrides"][0]["normalMax"], 4);
    let cluster =
        response_json(request(&ctx.app, "GET", &format!("/api/clusters/{id}"), None).await).await;
    assert_eq!(cluster["defaultNormalLagMax"], 12);
    let mut replacement = rules();
    replacement["overrides"] = json!([]);
    assert_eq!(
        request(&ctx.app, "PUT", &uri, Some(replacement))
            .await
            .status(),
        200
    );
    assert!(
        response_json(request(&ctx.app, "GET", &uri, None).await).await["overrides"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

// Invalid defaults or any invalid/duplicate override must leave both tables untouched.
#[tokio::test]
async fn invalid_rule_boundaries_and_duplicate_pairs_return_field_errors_without_changes() {
    let ctx = test_app().await;
    let id = create_cluster(&ctx.app, "prod").await;
    let uri = format!("/api/clusters/{id}/lag-rules");
    assert_eq!(
        request(&ctx.app, "PUT", &uri, Some(rules())).await.status(),
        200
    );
    let before = response_json(request(&ctx.app, "GET", &uri, None).await).await;
    for (scope, field, value) in [
        ("defaults", "normalMax", json!(-1)),
        ("defaults", "highMin", json!(12)),
        ("defaults", "overLimitSeconds", json!(-1)),
        ("override", "normalMax", json!(-1)),
        ("override", "highMin", json!(4)),
        ("override", "overLimitSeconds", json!(-1)),
        ("override", "groupId", json!(" ")),
        ("override", "topic", json!("")),
    ] {
        let mut input = rules();
        let field_name = if scope == "defaults" {
            input["defaults"][field] = value;
            format!("defaults.{field}")
        } else {
            input["overrides"][0][field] = value;
            format!("overrides.0.{field}")
        };
        let response = request(&ctx.app, "PUT", &uri, Some(input)).await;
        assert_eq!(response.status(), 422, "{field_name}");
        let body = response_json(response).await;
        assert_eq!(body["code"], "VALIDATION_ERROR");
        assert!(body["details"][&field_name].is_string(), "{body}");
    }
    let mut duplicate = rules();
    duplicate["overrides"]
        .as_array_mut()
        .unwrap()
        .push(rules()["overrides"][0].clone());
    let response = request(&ctx.app, "PUT", &uri, Some(duplicate)).await;
    assert_eq!(response.status(), 422);
    assert_eq!(response_json(response).await["code"], "VALIDATION_ERROR");
    assert_eq!(
        response_json(request(&ctx.app, "GET", &uri, None).await).await,
        before
    );
}

// A SQL failure after defaults and deletion must roll back every write in the PUT.
#[tokio::test]
async fn storage_failure_rolls_back_defaults_and_overrides_and_hides_database_details() {
    let ctx = test_app().await;
    let id = create_cluster(&ctx.app, "prod").await;
    let uri = format!("/api/clusters/{id}/lag-rules");
    request(&ctx.app, "PUT", &uri, Some(rules())).await;
    let before = response_json(request(&ctx.app, "GET", &uri, None).await).await;
    sqlx::query("CREATE TRIGGER reject_rule BEFORE INSERT ON lag_rules WHEN NEW.group_id = 'reject' BEGIN SELECT RAISE(ABORT, 'fixture-private-database-detail'); END").execute(&ctx.pool).await.unwrap();
    let mut input = rules();
    input["defaults"]["normalMax"] = json!(15);
    input["overrides"][0]["groupId"] = json!("reject");
    let response = request(&ctx.app, "PUT", &uri, Some(input)).await;
    assert_eq!(response.status(), 500);
    let text = response_text(response).await;
    assert!(!text.contains("fixture-private-database-detail"));
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap()["code"],
        "INTERNAL_ERROR"
    );
    assert_eq!(
        response_json(request(&ctx.app, "GET", &uri, None).await).await,
        before
    );
}

#[tokio::test]
async fn rules_for_missing_cluster_return_cluster_not_found() {
    let ctx = test_app().await;
    for method in ["GET", "PUT"] {
        let response = request(
            &ctx.app,
            method,
            "/api/clusters/missing/lag-rules",
            Some(rules()),
        )
        .await;
        assert_eq!(response.status(), 404);
        assert_eq!(response_json(response).await["code"], "CLUSTER_NOT_FOUND");
    }
}
