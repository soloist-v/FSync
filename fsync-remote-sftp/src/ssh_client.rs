use russh::client::{Handler, Session};
use russh::ChannelId;
use russh::keys::PublicKeyBase64;
use tracing::info;
pub(crate) struct Client {
    pub allowed_fingerprints: Option<Vec<String>>, // OpenSSH SHA256 base64 or raw base64 keys
}

impl Handler for Client {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        if let Some(allowed) = &self.allowed_fingerprints {
            // Russh provides SHA256 fingerprint (OpenSSH style) and base64 public key
            let fp_sha256 = server_public_key.fingerprint(russh::keys::HashAlg::Sha256).to_string();
            let key_b64 = server_public_key.public_key_base64();
            let ok = allowed.iter().any(|s| s == &fp_sha256 || s == &key_b64);
            info!("server key fp sha256: {}", fp_sha256);
            return Ok(ok);
        }
        info!("check_server_key (no whitelist): {:?}", server_public_key);
        Ok(true)
    }

    async fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        info!("data on channel {:?}: {}", channel, data.len());
        Ok(())
    }
}
