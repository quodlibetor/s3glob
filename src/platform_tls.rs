use std::fmt;
use std::sync::Arc;

use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_runtime_api::http::Response as SmithyResponse;
use aws_smithy_types::body::SdkBody;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector as HyperHttpConnector;
use hyper_util::rt::TokioExecutor;
use rustls_platform_verifier::BuilderVerifierExt as _;
use tower::Service as _;

type HyperClient = Client<HttpsConnector<HyperHttpConnector>, SdkBody>;

/// Builds a [`SharedHttpClient`] that verifies TLS certificates using the
/// platform's native trust store (macOS Keychain, Windows Cert Store, etc.)
/// via `rustls-platform-verifier`.
pub(crate) fn build() -> SharedHttpClient {
    let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let tls_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("TLS protocol versions should be valid")
        .with_platform_verifier()
        .expect("platform TLS verifier should initialize")
        .with_no_client_auth();
    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let client = Client::builder(TokioExecutor::new()).build(https_connector);
    SharedHttpClient::new(PlatformTlsClient {
        client: Arc::new(client),
    })
}

#[derive(Clone)]
struct PlatformTlsConnector {
    client: Arc<HyperClient>,
}

impl fmt::Debug for PlatformTlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlatformTlsConnector").finish()
    }
}

impl HttpConnector for PlatformTlsConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let request = match request.try_into_http1x() {
            Ok(r) => r,
            Err(e) => return HttpConnectorFuture::ready(Err(ConnectorError::user(e.into()))),
        };
        let mut client = (*self.client).clone();
        HttpConnectorFuture::new(async move {
            let response = client
                .call(request)
                .await
                .map_err(|e| ConnectorError::io(e.into()))?
                .map(SdkBody::from_body_1_x);
            SmithyResponse::try_from(response).map_err(|e| ConnectorError::other(e.into(), None))
        })
    }
}

#[derive(Clone, Debug)]
struct PlatformTlsClient {
    client: Arc<HyperClient>,
}

impl HttpClient for PlatformTlsClient {
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(PlatformTlsConnector {
            client: self.client.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_does_not_panic() {
        // Verifies that the platform TLS client initializes without error.
        // The actual certificate verification is delegated to the OS trust store,
        // so we just confirm the constructor succeeds.
        let _client = build();
    }
}
