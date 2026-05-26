//! Auth routes.
//!
//! Just `GET /api/auth/me` — used by the SPA to discover who the proxy says
//! the current user is. Login and logout are owned by the BFF (oauth2-proxy
//! at `/oauth2/sign_in` and `/oauth2/sign_out`); the backend doesn't issue
//! cookies anymore.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use surrealdb::types::ToSql;

use super::context::AuthContext;

pub async fn me(auth: Option<AuthContext>) -> Response {
    let Some(auth) = auth else {
        return (StatusCode::UNAUTHORIZED, "not authenticated").into_response();
    };
    Json(json!({
        "user": {
            "id": auth.user_id.to_sql(),
            "email": auth.email,
            "name": auth.display_name,
        },
        "tenant": {
            "id": auth.tenant_id.to_sql(),
        },
        "roles": auth.roles,
        "dev": auth.is_dev,
    }))
    .into_response()
}
