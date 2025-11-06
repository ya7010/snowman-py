mod error;
mod parameters;
pub mod query;
pub mod schema;

pub use error::Error;
pub use parameters::Parameters;
use snowflake_connector_rs::{
    SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeRow,
};

pub struct Connection {
    inner: SnowflakeClient,
}

impl Connection {
    pub fn try_new_by_password(
        username: String,
        password: String,
        account: String,
        warehouse: String,
        role: String,
        database: String,
        schema: Option<String>,
    ) -> Result<Self, snowflake_connector_rs::Error> {
        log::debug!("Using password authentication");
        log::debug!("username: {username}");
        log::debug!(
            "password: {}**********{}",
            &password[..2.min(password.len())],
            if password.len() >= 2 {
                &password[password.len() - 2..]
            } else {
                ""
            }
        );
        log::debug!("account: {account}");
        log::debug!("warehouse: {warehouse}");
        log::debug!("role: {role}");
        log::debug!("database: {database}");
        log::debug!("schema: {schema:?}");

        let client = SnowflakeClient::new(
            &username,
            SnowflakeAuthMethod::Password(password),
            SnowflakeClientConfig {
                account,
                warehouse: Some(warehouse),
                role: Some(role),
                database: Some(database),
                schema: schema.map(|s| s.to_string()),
                ..Default::default()
            },
        )?;
        Ok(Connection { inner: client })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_by_keypair(
        username: String,
        encrypted_pem: String,
        passphrase: Vec<u8>,
        account: String,
        warehouse: String,
        role: String,
        database: String,
        schema: Option<String>,
    ) -> Result<Self, snowflake_connector_rs::Error> {
        log::debug!("Using key pair authentication");
        log::debug!("username: {username}");
        log::debug!("passphrase provided: {}", !passphrase.is_empty());
        log::debug!("account: {account}");
        log::debug!("warehouse: {warehouse}");
        log::debug!("role: {role}");
        log::debug!("database: {database}");
        log::debug!("schema: {schema:?}");

        let client = SnowflakeClient::new(
            &username,
            SnowflakeAuthMethod::KeyPair {
                encrypted_pem,
                password: passphrase,
            },
            SnowflakeClientConfig {
                account,
                warehouse: Some(warehouse),
                role: Some(role),
                database: Some(database),
                schema: schema.map(|s| s.to_string()),
                ..Default::default()
            },
        )?;
        Ok(Connection { inner: client })
    }

    pub async fn execute(
        &self,
        query: &str,
    ) -> Result<Vec<SnowflakeRow>, snowflake_connector_rs::Error> {
        let session = self.inner.create_session().await?;
        session.query(query).await
    }
}
