use std::{fmt, time::Duration};

use rdkafka::ClientConfig;

use crate::domain::cluster::{DecryptedClusterConfig, SecurityProtocol};

/// Contains credentials. Its Debug implementation must never inspect the native config.
pub struct ClientProperties {
    pub(crate) inner: ClientConfig,
}

impl ClientProperties {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key)
    }
}

impl fmt::Debug for ClientProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientProperties([REDACTED])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Unsupported or incomplete Kafka security settings")]
pub struct ConfigError;

/// Validate combinations before passing any values to librdkafka.
pub fn build_client_properties(
    input: &DecryptedClusterConfig,
) -> Result<ClientProperties, ConfigError> {
    let sasl = matches!(
        input.security_protocol,
        SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
    );
    let tls = matches!(
        input.security_protocol,
        SecurityProtocol::Ssl | SecurityProtocol::SaslSsl
    );
    let timeout_ms = input.connection_timeout.as_millis();
    if input.bootstrap_servers.trim().is_empty()
        || input.connection_timeout < Duration::from_millis(1)
        || timeout_ms > i32::MAX as u128
        || [
            Some(input.bootstrap_servers.as_str()),
            input.sasl_mechanism.as_deref(),
            input.username.as_deref(),
            input.password.as_deref(),
            input.ca_pem.as_deref(),
            input.client_certificate_pem.as_deref(),
            input.client_key_pem.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|value| value.contains('\0'))
    {
        return Err(ConfigError);
    }
    if sasl {
        if !matches!(
            input.sasl_mechanism.as_deref(),
            Some("PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512")
        ) || input
            .username
            .as_deref()
            .is_none_or(|v| v.trim().is_empty())
            || input.password.as_deref().is_none_or(str::is_empty)
        {
            return Err(ConfigError);
        }
    } else if input.sasl_mechanism.is_some() || input.username.is_some() || input.password.is_some()
    {
        return Err(ConfigError);
    }
    if (!tls
        && (input.ca_pem.is_some()
            || input.client_certificate_pem.is_some()
            || input.client_key_pem.is_some()))
        || (input.client_certificate_pem.is_some() != input.client_key_pem.is_some())
        || [
            input.ca_pem.as_deref(),
            input.client_certificate_pem.as_deref(),
            input.client_key_pem.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|v| v.trim().is_empty())
    {
        return Err(ConfigError);
    }

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &input.bootstrap_servers)
        .set("security.protocol", input.security_protocol.as_str())
        .set("enable.auto.commit", "false")
        .set("allow.auto.create.topics", "false")
        .set(
            "socket.timeout.ms",
            timeout_ms.clamp(10, 300_000).to_string(),
        )
        .set(
            "socket.connection.setup.timeout.ms",
            timeout_ms.max(1_000).to_string(),
        );
    if sasl {
        config
            .set(
                "sasl.mechanism",
                input.sasl_mechanism.as_deref().ok_or(ConfigError)?,
            )
            .set(
                "sasl.username",
                input.username.as_deref().ok_or(ConfigError)?,
            )
            .set(
                "sasl.password",
                input.password.as_deref().ok_or(ConfigError)?,
            );
    }
    if tls {
        config
            .set("enable.ssl.certificate.verification", "true")
            .set("ssl.endpoint.identification.algorithm", "https");
        for (key, value) in [
            ("ssl.ca.pem", &input.ca_pem),
            ("ssl.certificate.pem", &input.client_certificate_pem),
            ("ssl.key.pem", &input.client_key_pem),
        ] {
            if let Some(value) = value {
                config.set(key, value);
            }
        }
    }
    Ok(ClientProperties { inner: config })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::cluster::{DecryptedClusterConfig, SecurityProtocol};
    use rdkafka::{
        admin::AdminClient, client::ClientContext, config::RDKafkaLogLevel, error::KafkaError,
    };
    use std::time::Duration;

    struct QuietContext;
    impl ClientContext for QuietContext {
        fn log(&self, _: RDKafkaLogLevel, _: &str, _: &str) {}
        fn error(&self, _: KafkaError, _: &str) {}
    }

    // Fixed public test certificate. Its private key was discarded at generation.
    const TEST_CA: &str = "-----BEGIN CERTIFICATE-----
MIIDBDCCAeygAwIBAgIUWTgAxMQvGXVMSbmSvYoQUMSV1+kwDQYJKoZIhvcNAQEL
BQAwIjEgMB4GA1UEAwwXa2Fma2EtZGFzaGJvYXJkLXRlc3QtY2EwHhcNMjYxMDE0
MTczNDA5WhcNMzYxMDExMTczNDA5WjAiMSAwHgYDVQQDDBdrYWZrYS1kYXNoYm9h
cmQtdGVzdC1jYTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAO0nEyze
KUcLeP0xM7fGfQChy7/6u2Qeii6AklmPEjQUBQA92/R+Uc1Xc9BUjffvLH6ViqwH
l30kYlptZgbJbnEi46sp2nS7b7s2qj7AN7ms/2YEqE2kpco6HH9jfbOMpFQsmsGX
OaE8ZE+rDdd6Kz6I5Oq1MQqNRa1TpOL9FsDBBXY/DV/9huzikN7DG9FCJIwLGoz/
pqvYeJuNHP+3qKqFgGfIlClqcHrAJbTrjIhrNaH3h/3TikGOguZ7Obl/LShNohc2
xcYQLUB+zpB+X1G16YPbo+RQ6LxnIc/qPqKjs/oZckRuTYSCeYHNM3GG+WTa55sa
/c5U0qEnb3Rxjj0CAwEAAaMyMDAwHQYDVR0OBBYEFF4kZmf46vj01+t4+OiawUp1
FQAQMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAGPPzydqJq1R
tc5J1erLR9BUUKo56HfGxSfVqhLp7HS3kzxxLoJYtxihXXW27UVpTZ4NrMWx6UZk
LkshdyoenPMMJnajRNgjiHXBwHKu7iVRxAwIWsZjXt8xLojsKqnolNJjCqwLn/CP
vDALsSAiy2rPtXxyjkcO41QfnxuauGFxNuo/9ZVigP8LK2QZYssO4oE7CkZDPb9W
4TLhGer+3rF6Lu+cZJIfSUv11HziQbF/EkPT+AsDnws+PWffRx+QM4294RoglBna
E0kRG74UTO1K1FIydgmRTHgQ3HyygnyQjX05WcibhfKlaweq4H79xagnSnM5xVBA
ZiyyKG9Z8lI=
-----END CERTIFICATE-----";

    fn fixture_cluster() -> DecryptedClusterConfig {
        DecryptedClusterConfig {
            bootstrap_servers: "kafka:9093".into(),
            security_protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some("SCRAM-SHA-512".into()),
            username: Some("reader".into()),
            password: Some("test-password".into()),
            ca_pem: Some(TEST_CA.into()),
            client_certificate_pem: None,
            client_key_pem: None,
            connection_timeout: Duration::from_secs(10),
        }
    }

    // A protocol/mechanism mismatch would silently weaken or break authentication.
    #[test]
    fn maps_all_supported_security_protocols_and_sasl_mechanisms() {
        for (protocol, expected) in [
            (SecurityProtocol::Plaintext, "PLAINTEXT"),
            (SecurityProtocol::Ssl, "SSL"),
            (SecurityProtocol::SaslPlaintext, "SASL_PLAINTEXT"),
            (SecurityProtocol::SaslSsl, "SASL_SSL"),
        ] {
            let sasl = matches!(
                protocol,
                SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
            );
            let tls = matches!(protocol, SecurityProtocol::Ssl | SecurityProtocol::SaslSsl);
            for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] {
                let mut input = fixture_cluster();
                input.security_protocol = protocol;
                input.sasl_mechanism = sasl.then(|| mechanism.into());
                if !sasl {
                    input.username = None;
                    input.password = None;
                }
                if !tls {
                    input.ca_pem = None;
                }
                let props = build_client_properties(&input).unwrap();
                assert_eq!(props.get("security.protocol"), Some(expected));
                assert_eq!(props.get("sasl.mechanism"), sasl.then_some(mechanism));
                assert_eq!(props.get("enable.auto.commit"), Some("false"));
                assert_eq!(props.get("socket.timeout.ms"), Some("10000"));
                assert_eq!(props.get("ssl.ca.pem"), tls.then_some(TEST_CA));
                assert!(!format!("{props:?}").contains("test-password"));
                // Exercise the compiled native feature set too: built-in PLAIN/SCRAM
                // must remain available with the gssapi/sasl Cargo features disabled.
                let client: Result<AdminClient<QuietContext>, _> =
                    props.inner.create_with_context(QuietContext);
                assert!(
                    client.is_ok(),
                    "supported security configuration could not create a native client"
                );
                drop(client);
            }
        }
    }

    // Unsupported authentication must fail before constructing a native client.
    #[test]
    fn rejects_invalid_combinations_without_echoing_values() {
        let mut cases = Vec::new();
        for mechanism in [
            None,
            Some("GSSAPI"),
            Some("OAUTHBEARER"),
            Some("test-password"),
        ] {
            let mut input = fixture_cluster();
            input.sasl_mechanism = mechanism.map(str::to_owned);
            cases.push(input);
        }
        let mut no_user = fixture_cluster();
        no_user.username = None;
        cases.push(no_user);
        let mut no_password = fixture_cluster();
        no_password.password = None;
        cases.push(no_password);
        let mut no_tls = fixture_cluster();
        no_tls.security_protocol = SecurityProtocol::SaslPlaintext;
        cases.push(no_tls);
        let mut no_sasl = fixture_cluster();
        no_sasl.security_protocol = SecurityProtocol::Ssl;
        cases.push(no_sasl);
        let mut unpaired = fixture_cluster();
        unpaired.client_key_pem = Some("private-key".into());
        cases.push(unpaired);
        let mut zero_timeout = fixture_cluster();
        zero_timeout.connection_timeout = Duration::ZERO;
        cases.push(zero_timeout);
        let mut huge_timeout = fixture_cluster();
        huge_timeout.connection_timeout = Duration::MAX;
        cases.push(huge_timeout);
        let mut nul_password = fixture_cluster();
        nul_password.password = Some("test-password\0".into());
        cases.push(nul_password);
        for input in cases {
            let error = match build_client_properties(&input) {
                Ok(_) => panic!("invalid configuration accepted"),
                Err(error) => error,
            };
            assert!(!format!("{error:?} {error}").contains("test-password"));
        }
    }

    #[test]
    fn maps_paired_mutual_tls_credentials() {
        let mut input = fixture_cluster();
        input.client_certificate_pem = Some("public-client-cert".into());
        input.client_key_pem = Some("private-client-key".into());
        let props = build_client_properties(&input).unwrap();
        assert_eq!(props.get("ssl.certificate.pem"), Some("public-client-cert"));
        assert!(props.get("ssl.key.pem").is_some());
        assert_eq!(
            props.get("enable.ssl.certificate.verification"),
            Some("true")
        );
        assert_eq!(
            props.get("ssl.endpoint.identification.algorithm"),
            Some("https")
        );
        assert!(!format!("{props:?}").contains("private-client-key"));
    }

    // The API accepts every positive millisecond timeout, including values below
    // librdkafka's socket-timeout minimum. The request deadline must still work.
    #[test]
    fn accepts_positive_timeouts_outside_native_socket_range() {
        for (millis, expected_socket_ms) in [(1, "10"), (600_000, "300000")] {
            let mut input = DecryptedClusterConfig::plaintext("127.0.0.1:9092");
            input.connection_timeout = Duration::from_millis(millis);
            let props = build_client_properties(&input).unwrap();
            assert_eq!(props.get("socket.timeout.ms"), Some(expected_socket_ms));
        }
    }
}
