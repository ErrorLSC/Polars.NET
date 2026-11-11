//! Credentials the host renews: a table whose credentials expire (as a catalog vends them) names a host credential
//! source in its cloud options; the object stores then ask the host for credentials whenever the ones they hold
//! reach the time the host said to ask again.

use std::sync::{Arc, RwLock};

use polars::error::{PolarsError, PolarsResult};
use polars::prelude::CloudScheme;
use polars_io::cloud::credential_provider::{ObjectStoreCredential, PlCredentialProvider};

/// The configuration key naming a host credential source; its value is the source's id. Never handed to the
/// object store library.
pub(crate) const CREDENTIAL_SOURCE_KEY: &str = "polars_net_credential_source";

/// Fills `out` with the source's current credentials as `key TAB value` lines (object_store configuration keys)
/// and `out_renew_at` with the time to ask again, in seconds since the epoch (`u64::MAX`: never). Returns 0 on
/// success; 1 on failure, with the message in `out`; 2 when `out` is too small, with the size needed in `out_len`.
pub type FfiCredentialCallback =
    extern "C" fn(source: u64, out: *mut u8, capacity: usize, out_len: *mut usize, out_renew_at: *mut u64) -> i32;

static CALLBACK: RwLock<Option<FfiCredentialCallback>> = RwLock::new(None);

/// Registers the host function that serves credential sources (once per process; later calls replace it).
#[unsafe(no_mangle)]
pub extern "C" fn pl_cloud_set_credential_callback(callback: FfiCredentialCallback) {
    *CALLBACK.write().unwrap_or_else(|e| e.into_inner()) = Some(callback);
}

fn err(message: impl Into<String>) -> PolarsError {
    PolarsError::ComputeError(message.into().into())
}

/// Calls the host: the credentials as configuration pairs, and when to ask again.
fn fetch(source: u64) -> PolarsResult<(Vec<(String, String)>, u64)> {
    let callback = (*CALLBACK.read().unwrap_or_else(|e| e.into_inner()))
        .ok_or_else(|| err("No host credential callback is registered."))?;
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        let mut len = 0usize;
        let mut renew_at = 0u64;
        let status = callback(source, buffer.as_mut_ptr(), buffer.len(), &mut len, &mut renew_at);
        match status {
            0 | 1 => {
                let text = String::from_utf8_lossy(&buffer[..len.min(buffer.len())]).into_owned();
                if status == 1 {
                    return Err(err(text));
                }
                let pairs = text
                    .lines()
                    .filter_map(|line| line.split_once('\t'))
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();
                return Ok((pairs, renew_at));
            },
            2 if len > buffer.len() => buffer.resize(len, 0),
            _ => return Err(err(format!("The host credential callback failed (status {status})."))),
        }
    }
}

fn value<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.as_str())
}

/// A SAS token as query pairs: percent-decoded, split at `&` and the first `=` (as object_store reads one).
fn sas_pairs(token: &str) -> PolarsResult<Vec<(String, String)>> {
    let bytes = token.trim_start_matches('?').as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                decoded.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        decoded.push(bytes[i]);
        i += 1;
    }
    let text = String::from_utf8(decoded).map_err(|_| err("The SAS token is not UTF-8 once decoded."))?;
    text.split('&')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .ok_or_else(|| err("A component of the SAS token has no value."))
        })
        .collect()
}

/// The credentials for the store of `scheme`: an error when the host's are for another cloud.
fn credential(scheme: Option<CloudScheme>, pairs: &[(String, String)]) -> PolarsResult<ObjectStoreCredential> {
    match scheme {
        Some(CloudScheme::S3 | CloudScheme::S3a) => {
            let (Some(key_id), Some(secret_key)) = (value(pairs, "aws_access_key_id"), value(pairs, "aws_secret_access_key"))
            else {
                return Err(err("The renewed credentials are not AWS credentials, which the S3 store needs."));
            };
            Ok(ObjectStoreCredential::Aws(Arc::new(polars_io::cloud::credential_provider::AwsCredential {
                key_id: key_id.to_string(),
                secret_key: secret_key.to_string(),
                token: value(pairs, "aws_session_token").map(str::to_string),
            })))
        },
        Some(CloudScheme::Azure | CloudScheme::Abfs | CloudScheme::Abfss | CloudScheme::Adl | CloudScheme::Az) => {
            use polars_io::cloud::credential_provider::AzureCredential;
            if let Some(sas) = value(pairs, "azure_storage_sas_token") {
                Ok(ObjectStoreCredential::Azure(Arc::new(AzureCredential::SASToken(sas_pairs(sas)?))))
            } else if let Some(token) = value(pairs, "azure_storage_token") {
                Ok(ObjectStoreCredential::Azure(Arc::new(AzureCredential::BearerToken(token.to_string()))))
            } else {
                Err(err("The renewed credentials are not Azure credentials, which the Azure store needs."))
            }
        },
        Some(CloudScheme::Gs | CloudScheme::Gcs) => match value(pairs, "google_oauth_token") {
            Some(bearer) => Ok(ObjectStoreCredential::Gcp(Arc::new(polars_io::cloud::credential_provider::GcpCredential {
                bearer: bearer.to_string(),
            }))),
            None => Err(err("The renewed credentials are not a Google OAuth token, which the GCS store needs.")),
        },
        _ => Err(err("Renewed credentials are only supported for S3, Azure and GCS.")),
    }
}

/// A provider asking the host source `source` for credentials. The host call blocks (it may go to the network),
/// so it runs on its own thread, not on the async worker.
pub(crate) fn host_credential_provider(source: u64, scheme: Option<CloudScheme>) -> PlCredentialProvider {
    PlCredentialProvider::from_func(move || {
        Box::pin(async move {
            let (sender, receiver) = futures::channel::oneshot::channel();
            std::thread::Builder::new()
                .name("credential-renewal".into())
                .spawn(move || {
                    let _ = sender.send(fetch(source).and_then(|(pairs, renew_at)| Ok((credential(scheme, &pairs)?, renew_at))));
                })
                .map_err(|e| err(format!("Cannot start the credential renewal thread: {e}")))?;
            receiver.await.map_err(|_| err("The credential renewal thread ended without an answer."))?
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sas_tokens_are_decoded_into_pairs() {
        let pairs = sas_pairs("?sv=2022-11-02&sig=a%2Bb%3D&se=2026-01-01T00%3A00%3A00Z").unwrap();
        assert_eq!(
            pairs,
            vec![
                ("sv".to_string(), "2022-11-02".to_string()),
                ("sig".to_string(), "a+b=".to_string()),
                ("se".to_string(), "2026-01-01T00:00:00Z".to_string()),
            ]
        );
        assert!(sas_pairs("sv").is_err());
    }
}
