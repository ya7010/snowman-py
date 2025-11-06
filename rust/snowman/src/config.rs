use snowman_config::Config;
use snowman_generator::PydanticOptions;

pub fn get_pydantic_options(config: &Config) -> PydanticOptions {
    let Config {
        pydantic: pydantic_options,
        ..
    } = config;

    PydanticOptions {
        model_name_prefix: pydantic_options.model_name_prefix.clone(),
        model_name_suffix: pydantic_options.model_name_suffix.clone(),
    }
}

pub fn get_model_output_dirpath(config: &Config) -> std::path::PathBuf {
    let Config {
        model: snowman_config::ModelConfig { output_dir, .. },
        ..
    } = config;

    output_dir.clone()
}

pub fn get_snowflake_connection(
    config: &Config,
) -> Result<snowman_connector::Connection, anyhow::Error> {
    let Config { connection, .. } = config;

    let username = connection.user.try_get_value()?;
    let account = connection.account.try_get_value()?;
    let warehouse = connection.warehouse.try_get_value()?;
    let role = connection.role.try_get_value()?;
    let database = connection.database.try_get_value()?;
    let schema = connection
        .schema
        .as_ref()
        .and_then(|v| v.try_get_value().ok());

    if let Ok(private_key) = connection.private_key.try_get_value() {
        snowman_connector::Connection::try_new_by_keypair(
            username,
            private_key,
            get_passphrase(config),
            account,
            warehouse,
            role,
            database,
            schema,
        )
    } else if let Ok(private_key_path) = connection.private_key_path.try_get_value() {
        let encrypted_pem = std::fs::read_to_string(&private_key_path)
            .map_err(|e| anyhow::anyhow!("Failed to read private key file: {}", e))?;

        snowman_connector::Connection::try_new_by_keypair(
            username,
            encrypted_pem,
            get_passphrase(config),
            account,
            warehouse,
            role,
            database,
            schema,
        )
    } else {
        // パスワード認証
        let password = connection.password.try_get_value()?;

        snowman_connector::Connection::try_new_by_password(
            username, password, account, warehouse, role, database, schema,
        )
    }
    .map_err(Into::into)
}

fn get_passphrase(config: &Config) -> Vec<u8> {
    let Config { connection, .. } = config;

    connection
        .private_key_passphrase
        .try_get_value()
        .unwrap_or_default()
        .into_bytes()
}
