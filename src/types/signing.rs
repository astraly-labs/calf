use std::fmt::Debug;
use std::fmt::Formatter;

use async_trait::async_trait;
use bincode::ErrorKind;
use libp2p::identity::{Keypair, SigningError};
use serde::{Deserialize, Serialize};

pub type Signature = Vec<u8>;

#[derive(Debug, thiserror::Error)]
pub enum SignError {
    #[error(transparent)]
    Serialize(#[from] ErrorKind),
    #[error(transparent)]
    Sign(#[from] SigningError),
}

/// A type that can be signed
/// The content will be
#[async_trait]
pub trait Signable: Sized + Serialize {
    fn sign(&self, keypair: &Keypair) -> Result<Signature, SignError> {
        let msg = bincode::serialize(self).map_err(|e| SignError::Serialize(*e))?;
        keypair.sign(&msg).map_err(SignError::Sign)
    }
}

/// A signed type. Contains the original value and the signature.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedType<T: Signable> {
    /// The value which was signed
    #[serde(alias = "header")]
    pub value: T,
    /// The signature for the value
    pub signature: Signature,
}

pub fn sign_with_keypair<T: Signable + Send>(
    keypair: &Keypair,
    value: T,
) -> Result<SignedType<T>, SignError> {
    let signature = value.sign(&keypair)?;

    Ok(SignedType { value, signature })
}

impl<T: Signable + Debug> Debug for SignedType<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SignedType {{ value: {:?}, signature: 0x{:?} }}",
            self.value, self.signature
        )
    }
}
