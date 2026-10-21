//! A fault-injecting HTTP proxy for exercising s3glob's error handling.
//!
//! The proxy forwards requests to an S3-compatible backend (MinIO in these
//! tests) unchanged, except for requests matched by a [`Fault`], which get a
//! `503 SlowDown` response instead. The `Host` header is forwarded as-is so
//! SigV4 signatures computed against the proxy's address still validate.

#![allow(dead_code)] // each test crate uses a different subset

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

/// Fail ListObjectsV2 requests whose `prefix` parameter starts with
/// `prefix`.
#[derive(Clone, Debug)]
pub struct Fault {
    pub prefix: String,
    /// How many matching requests to fail before letting them through.
    /// `None` fails every matching request.
    pub times: Option<usize>,
    /// Sent as `x-amz-retry-after` (milliseconds) when set.
    pub retry_after_ms: Option<u64>,
}

impl Fault {
    pub fn always(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_owned(),
            times: None,
            retry_after_ms: None,
        }
    }

    pub fn times(prefix: &str, times: usize) -> Self {
        Self {
            prefix: prefix.to_owned(),
            times: Some(times),
            retry_after_ms: None,
        }
    }
}

#[derive(Default)]
struct State {
    faults: Vec<Fault>,
    /// Number of requests failed by each fault, by index.
    injected: Vec<usize>,
}

pub struct FaultProxy {
    pub port: u16,
    state: Arc<Mutex<State>>,
}

impl FaultProxy {
    pub async fn start(backend_port: u16, faults: Vec<Fault>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(State {
            injected: vec![0; faults.len()],
            faults,
        }));
        let client: Client<HttpConnector, Incoming> =
            Client::builder(TokioExecutor::new()).build_http();
        let backend: SocketAddr = ([127, 0, 0, 1], backend_port).into();

        let accept_state = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let state = accept_state.clone();
                let client = client.clone();
                tokio::spawn(async move {
                    let service =
                        service_fn(move |req| handle(req, state.clone(), client.clone(), backend));
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Self { port, state }
    }

    /// How many requests each fault (in construction order) has failed.
    pub fn injected(&self) -> Vec<usize> {
        self.state.lock().unwrap().injected.clone()
    }
}

async fn handle(
    mut req: Request<Incoming>,
    state: Arc<Mutex<State>>,
    client: Client<HttpConnector, Incoming>,
    backend: SocketAddr,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if let Some(fault) = take_fault(&req, &state) {
        let mut resp = Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("content-type", "application/xml");
        if let Some(ms) = fault.retry_after_ms {
            resp = resp.header("x-amz-retry-after", ms.to_string());
        }
        let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
            <Error><Code>SlowDown</Code>\
            <Message>Please reduce your request rate.</Message>\
            <RequestId>fault-proxy</RequestId></Error>";
        return Ok(resp.body(Full::new(Bytes::from(body))).unwrap());
    }

    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_owned())
        .unwrap_or_else(|| "/".to_owned());
    *req.uri_mut() = format!("http://{backend}{path_and_query}").parse().unwrap();
    match client.request(req).await {
        Ok(resp) => {
            let (parts, body) = resp.into_parts();
            let body = body
                .collect()
                .await
                .map(|b| b.to_bytes())
                .unwrap_or_default();
            Ok(Response::from_parts(parts, Full::new(body)))
        }
        Err(e) => Ok(Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Full::new(Bytes::from(e.to_string())))
            .unwrap()),
    }
}

fn take_fault(req: &Request<Incoming>, state: &Mutex<State>) -> Option<Fault> {
    let query = req.uri().query()?;
    let prefix = url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == "prefix")
        .map(|(_, v)| v.into_owned())?;
    let mut state = state.lock().unwrap();
    let State { faults, injected } = &mut *state;
    for (fault, count) in faults.iter().zip(injected.iter_mut()) {
        if prefix.starts_with(&fault.prefix) && fault.times.is_none_or(|t| *count < t) {
            *count += 1;
            return Some(fault.clone());
        }
    }
    None
}
