use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};
use base64::{Engine, engine::general_purpose::STANDARD};

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("master key must be base64 encoding of exactly 32 bytes")]
    InvalidKey,
    #[error("invalid encrypted secret format")]
    InvalidFormat,
    #[error("secret authentication failed")]
    Authentication,
    #[error("secret encryption failed")]
    Encryption,
}

#[derive(Clone)]
pub struct SecretCipher {
    inner: Aes256Gcm,
}

impl std::fmt::Debug for SecretCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretCipher([REDACTED])")
    }
}

impl SecretCipher {
    pub fn from_key(key: [u8; 32]) -> Self {
        Self {
            inner: Aes256Gcm::new(&key.into()),
        }
    }

    pub fn from_base64_key(encoded: &str) -> Result<Self, CryptoError> {
        let decoded = STANDARD
            .decode(encoded)
            .map_err(|_| CryptoError::InvalidKey)?;
        let key: [u8; 32] = decoded.try_into().map_err(|_| CryptoError::InvalidKey)?;
        Ok(Self::from_key(key))
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let nonce_bytes: [u8; 12] = rand::random();
        let encrypted = self
            .inner
            .encrypt(&nonce_bytes.into(), plaintext)
            .map_err(|_| CryptoError::Encryption)?;
        Ok([&[1_u8][..], &nonce_bytes, &encrypted].concat())
    }

    pub fn decrypt(&self, encrypted: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if encrypted.len() < 1 + 12 + 16 || encrypted[0] != 1 {
            return Err(CryptoError::InvalidFormat);
        }
        let nonce_bytes: [u8; 12] = encrypted[1..13]
            .try_into()
            .map_err(|_| CryptoError::InvalidFormat)?;
        self.inner
            .decrypt(&nonce_bytes.into(), &encrypted[13..])
            .map_err(|_| CryptoError::Authentication)
    }
}
