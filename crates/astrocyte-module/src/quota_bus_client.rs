//! Attested ckbus management calls and seedless NATS authentication.
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use cortexkit_bus_nats::{ConnectConfig, NatsConnection};
use serde::Deserialize;
use serde_json::{json, Value};
use subc_client_rs::{CallOptions, ConsumerOptions, RouteHandle, SubcConsumer};
use subc_protocol::RouteTarget;
use tokio::sync::{watch, Mutex};

const SUPERSEDED: &str = "ckbus_credential_superseded";
const REVOKED: &str = "ckbus_credential_revoked";
const MAX_RECONNECTS: usize = 18;

/// The nonce is shared with HELLO, never rediscovered or included in diagnostics.
#[derive(Clone)]
pub(crate) struct Access {
    pub path: PathBuf,
    pub module_id: String,
    pub nonce: String,
}

struct Route {
    consumer: SubcConsumer,
    handle: RouteHandle,
}

pub(crate) struct Client {
    access: Access,
    route: Option<Route>,
}

#[derive(Clone, Deserialize)]
pub(crate) struct Credential {
    pub jwt: String,
    pub exp: i64,
    pub acct: String,
    pub inbox_prefix: String,
    pub server_url: String,
    pub credential_public: String,
    pub spawn_generation: u64,
    pub credential_epoch: u64,
}

impl Credential {
    fn validate(&self) -> Result<(), &'static str> {
        let config =
            ConnectConfig::new(&self.credential_public).map_err(|_| "invalid credential inbox")?;
        cortexkit_bus_naming::AccountNames::derive(&self.acct)
            .map_err(|_| "invalid credential account")?;
        if self.jwt.is_empty()
            || self.server_url.is_empty()
            || self.inbox_prefix != config.inbox_prefix()
            || self.exp <= chrono::Utc::now().timestamp().saturating_add(60)
        {
            return Err("invalid or expired credential");
        }
        Ok(())
    }
}

impl Client {
    pub fn new(access: Access) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            access,
            route: None,
        }))
    }

    async fn open(&self) -> Result<Route, &'static str> {
        // The SDK binds supervised connections to a fresh claim from the serving owner.
        let consumer = SubcConsumer::connect(&self.access.path, ConsumerOptions::default())
            .await
            .map_err(|_| "ckbus connection failed")?;
        let identity =
            serde_json::from_value(json!({"harness":"ck-astrocyte","session":"quota-events"}))
                .map_err(|_| "ckbus route encoding failed")?;
        let handle = consumer
            .open_route(
                RouteTarget::ManagementSurface {
                    module_id: "ckbus".into(),
                },
                identity,
                options(),
            )
            .await
            .map_err(safe_call_error)?;
        Ok(Route { consumer, handle })
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, &'static str> {
        // This route conveys a launch credential and must never become a general peer client.
        if !matches!(
            method,
            "ckbus.credential" | "ckbus.credential_renew" | "ckbus.nonce_sign"
        ) {
            return Err("unsupported ckbus operation");
        }
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            if self.route.is_none() {
                self.route = Some(self.open().await?);
            }
            let body = serde_json::to_vec(&json!({"method": method, "params": params}))
                .map_err(|_| "ckbus request encoding failed")?;
            let response = exchange(self.route.as_mut().expect("route installed"), body).await?;
            response.get("result").cloned().ok_or("ckbus result absent")
        })
        .await
        .unwrap_or(Err("ckbus request deadline"));
        if result.is_err() {
            self.route = None;
        }
        result
    }
}

async fn exchange(route: &mut Route, body: Vec<u8>) -> Result<Value, &'static str> {
    let response = route
        .consumer
        .request(&route.handle, body, options())
        .await
        .map_err(safe_call_error)?;
    serde_json::from_slice(&response).map_err(|_| "invalid ckbus reply")
}

fn options() -> CallOptions {
    CallOptions {
        timeout: Duration::from_secs(2),
        route_retry_deadline: Duration::from_secs(2),
        ..CallOptions::default()
    }
}

fn safe_call_error(error: subc_client_rs::CallError) -> &'static str {
    // Server error text can echo request data. Only fixed structural codes leave this boundary.
    match error
        .route_open_refusal()
        .map(|body| body.code.as_str())
        .or_else(|| error.code())
    {
        Some(SUPERSEDED) => SUPERSEDED,
        Some(REVOKED) => REVOKED,
        Some("bad_consumer_identity") => "ckbus identity refused",
        Some("ckbus_principal_direct") => "ckbus principal refused",
        _ => "ckbus operation unavailable",
    }
}

pub(crate) async fn credential(
    client: &Arc<Mutex<Client>>,
    prior: Option<&Credential>,
) -> Result<Credential, &'static str> {
    let mut client = client.lock().await;
    if let Some(prior) = prior {
        match client
            .call(
                "ckbus.credential_renew",
                json!({"credential_public": prior.credential_public}),
            )
            .await
        {
            Ok(value) => {
                if value["credential_public"] != prior.credential_public
                    || value["spawn_generation"].as_u64() != Some(prior.spawn_generation)
                    || value["credential_epoch"].as_u64() != Some(prior.credential_epoch)
                {
                    return Err("ckbus renewal changed identity");
                }
                let mut renewed = prior.clone();
                renewed.jwt = value["jwt"]
                    .as_str()
                    .ok_or("renewal JWT absent")?
                    .to_owned();
                renewed.exp = value["exp"]
                    .as_i64()
                    .filter(|exp| *exp > prior.exp)
                    .ok_or("renewal did not extend expiry")?;
                renewed.validate()?;
                return Ok(renewed);
            }
            Err(SUPERSEDED | REVOKED) => {}
            Err(reason) => return Err(reason),
        }
    }
    let value = client.call("ckbus.credential", json!({})).await?;
    let credential: Credential =
        serde_json::from_value(value).map_err(|_| "invalid ckbus credential")?;
    credential.validate()?;
    Ok(credential)
}

pub(crate) struct SessionEnd(watch::Receiver<Option<&'static str>>);

impl SessionEnd {
    pub(crate) async fn reason(mut self) -> &'static str {
        match self.0.wait_for(Option::is_some).await {
            Ok(refusal) => refusal.expect("waited for a refusal"),
            // The connector owns the sender; it drops it when it stops reconnecting.
            Err(_) => "NATS connection closed",
        }
    }
}

fn reconnect_delay(attempt: usize) -> Duration {
    if attempt <= 1 {
        Duration::ZERO
    } else {
        Duration::from_millis((1_u64 << (attempt - 1).min(12)).min(4_000))
    }
}

pub(crate) async fn connect(
    client: Arc<Mutex<Client>>,
    credential: &Credential,
) -> Result<(NatsConnection, SessionEnd), &'static str> {
    let jwt = credential.jwt.clone();
    let key = credential.credential_public.clone();
    let (refusal, session_end) = watch::channel(None);
    let options =
        async_nats::ConnectOptions::with_auth_callback(move |nonce| {
            let (client, jwt, key) = (client.clone(), jwt.clone(), key.clone());
            let refusal = refusal.clone();
            async move {
                if refusal.is_closed() || refusal.borrow().is_some() {
                    return Err(async_nats::AuthError::new("bus session ended"));
                }
                // NATS requires a Sync authentication future; the SDK connection future
                // is Send but not Sync. A task boundary keeps its transport private.
                let value =
                    tokio::spawn(async move {
                        client.lock().await.call(
                    "ckbus.nonce_sign",
                    json!({"credential_public": key, "nonce_b64": STANDARD.encode(nonce)}),
                ).await
                    })
                    .await
                    .map_err(|_| async_nats::AuthError::new("signing task unavailable"))?
                    .map_err(|reason| {
                        if matches!(reason, SUPERSEDED | REVOKED) {
                            refusal.send_replace(Some(reason));
                        }
                        async_nats::AuthError::new(reason)
                    })?;
                let signature = value["signature_b64"]
                    .as_str()
                    .ok_or_else(|| async_nats::AuthError::new("signature absent"))?;
                let signature = STANDARD
                    .decode(signature)
                    .map_err(|_| async_nats::AuthError::new("invalid signature"))?;
                if signature.len() != 64 {
                    return Err(async_nats::AuthError::new("invalid signature length"));
                }
                let mut auth = async_nats::Auth::new();
                auth.jwt = Some(jwt);
                auth.signature = Some(signature);
                Ok(auth)
            }
        })
        // Keep the 0.50 retry schedule: immediate, then 2, 4, 8, ... ms up to 4 s.
        // Eighteen attempts span 28 s even with instant dial failures, enough to
        // reuse the credential through a normal ck-bus restart (~20 s).
        // async-nats 0.50 has no public abort for its reconnect loop. Dropping the
        // client does not stop that loop, so cap it to retire failed sessions.
        .reconnect_delay_callback(reconnect_delay)
        .max_reconnects(MAX_RECONNECTS)
        .connection_timeout(Duration::from_secs(2));
    let connection = NatsConnection::connect(
        &credential.server_url,
        options,
        ConnectConfig::new(&credential.credential_public)
            .map_err(|_| "invalid credential inbox")?,
    )
    .await
    .map_err(|_| session_end.borrow().unwrap_or("NATS connection failed"))?;
    Ok((connection, SessionEnd(session_end)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_budget_covers_a_normal_bus_restart() {
        let wait_before_last_attempt: Duration = (1..=MAX_RECONNECTS).map(reconnect_delay).sum();
        assert!(wait_before_last_attempt > Duration::from_secs(20));
    }
}
