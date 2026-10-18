//! Attested ckbus management calls and seedless NATS authentication.
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use cortexkit_bus_nats::{ConnectConfig, NatsConnection};
use serde::Deserialize;
use serde_json::{json, Value};
use subc_protocol::FrameType;
use subc_transport::{authenticate_client, connection_file};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

/// The nonce is shared with HELLO, never rediscovered or included in diagnostics.
#[derive(Clone)]
pub(crate) struct Access {
    pub path: PathBuf,
    pub module_id: String,
    pub nonce: String,
}

struct Route {
    stream: TcpStream,
    channel: u16,
    epoch: u32,
    corr: u64,
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
        let metadata = connection_file::read(&self.access.path)
            .map_err(|_| "ckbus connection file unavailable")?;
        let endpoint = metadata.endpoints.first().ok_or("ckbus endpoint absent")?;
        let mut stream = crate::peer_exchange::connect_endpoint(&endpoint.host, endpoint.port)
            .await
            .map_err(|_| "ckbus connection failed")?;
        authenticate_client(&mut stream, &metadata, Duration::from_secs(2))
            .await
            .map_err(|_| "ckbus transport authentication failed")?;
        let mut route = Route {
            stream,
            channel: 0,
            epoch: 0,
            corr: 1,
        };
        let body = serde_json::to_vec(&json!({
            "op": "route.open",
            "target": {"kind": "management_surface", "module_id": "ckbus"},
            "identity": {"project_root": "/", "harness": "ck-astrocyte", "session": "quota-events"},
            "consumer_identity": {"module_id": self.access.module_id, "launch_nonce": self.access.nonce},
        })).map_err(|_| "ckbus route encoding failed")?;
        let value = exchange(&mut route, body).await?;
        route.channel = value["route_channel"]
            .as_u64()
            .and_then(|v| u16::try_from(v).ok())
            .filter(|v| *v != 0)
            .ok_or("invalid ckbus route channel")?;
        route.epoch = value["route_epoch"]
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .filter(|v| *v != 0)
            .ok_or("invalid ckbus route epoch")?;
        Ok(route)
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
    let corr = route.corr;
    route.corr = route
        .corr
        .checked_add(1)
        .ok_or("ckbus correlation exhausted")?;
    let response = crate::peer_exchange::exchange(
        &mut route.stream,
        route.channel,
        route.epoch,
        corr,
        body,
        crate::peer_exchange::ExchangeMode::Peer,
    )
    .await
    .map_err(|_| "ckbus exchange failed")?;
    let value: Value = serde_json::from_slice(&response.body).map_err(|_| "invalid ckbus reply")?;
    if response.header.ty == FrameType::Error {
        // Server error text can echo request data. Only fixed structural codes leave this boundary.
        return Err(match value["code"].as_str() {
            Some("ckbus_credential_superseded") => "ckbus_credential_superseded",
            Some("ckbus_credential_revoked") => "ckbus_credential_revoked",
            Some("bad_consumer_identity") => "ckbus identity refused",
            Some("ckbus_principal_direct") => "ckbus principal refused",
            _ => "ckbus operation unavailable",
        });
    }
    Ok(value)
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
            Err("ckbus_credential_superseded" | "ckbus_credential_revoked") => {}
            Err(reason) => return Err(reason),
        }
    }
    let value = client.call("ckbus.credential", json!({})).await?;
    let credential: Credential =
        serde_json::from_value(value).map_err(|_| "invalid ckbus credential")?;
    credential.validate()?;
    Ok(credential)
}

pub(crate) async fn connect(
    client: Arc<Mutex<Client>>,
    credential: &Credential,
) -> Result<NatsConnection, &'static str> {
    let jwt = credential.jwt.clone();
    let key = credential.credential_public.clone();
    let options = async_nats::ConnectOptions::with_auth_callback(move |nonce| {
        let (client, jwt, key) = (client.clone(), jwt.clone(), key.clone());
        async move {
            let value = client
                .lock()
                .await
                .call(
                    "ckbus.nonce_sign",
                    json!({
                        "credential_public": key, "nonce_b64": STANDARD.encode(nonce),
                    }),
                )
                .await
                .map_err(async_nats::AuthError::new)?;
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
    .max_reconnects(0)
    .connection_timeout(Duration::from_secs(2));
    NatsConnection::connect(
        &credential.server_url,
        options,
        ConnectConfig::new(&credential.credential_public)
            .map_err(|_| "invalid credential inbox")?,
    )
    .await
    .map_err(|_| "NATS connection failed")
}
