mod error;

use std::io::Write;

use serde::{Deserialize, Serialize};

pub use error::Error;

const CONFIG_FILENAME: &str = "snowman.toml";
const PYPROJECT_FILENAME: &str = "pyproject.toml";

#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// # The Snowflake connection configuration.
    #[serde(default)]
    pub connection: ConnectionConfig,

    /// # The Python model configuration.
    ///
    /// It is mainly used for the Snowman model generate command.
    #[serde(default)]
    pub model: ModelConfig,

    /// # The Pydantic options.
    #[serde(default)]
    pub pydantic: PydanticConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    /// # The Snowflake account name.
    #[serde(default = "account_default")]
    pub account: StringOrEnv,

    /// # The Snowflake user name.
    #[serde(default = "user_default")]
    pub user: StringOrEnv,

    /// # The Snowflake password.
    ///
    /// Currently only password authentication is supported.
    #[serde(default = "password_default")]
    pub password: StringOrEnv,

    /// # The Snowflake private key.
    #[serde(default = "private_key_default")]
    pub private_key: StringOrEnv,

    /// # The Snowflake private key path.
    #[serde(default = "private_key_path_default")]
    pub private_key_path: StringOrEnv,

    /// # The Snowflake private key passphrase.
    #[serde(default = "private_key_passphrase_default")]
    pub private_key_passphrase: StringOrEnv,

    /// # The Snowflake role.
    #[serde(default = "role_default")]
    pub role: StringOrEnv,

    /// # The Snowflake warehouse.
    #[serde(default = "warehouse_default")]
    pub warehouse: StringOrEnv,

    /// # The Snowflake database.
    #[serde(default = "database_default")]
    pub database: StringOrEnv,

    /// # The Snowflake schema.
    ///
    /// This is optional.
    /// Schema is not required for this tool, but it can be set for consistency.
    #[serde(default)]
    pub schema: Option<StringOrEnv>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        ConnectionConfig {
            account: account_default(),
            user: user_default(),
            password: password_default(),
            private_key: private_key_default(),
            private_key_path: private_key_path_default(),
            private_key_passphrase: private_key_passphrase_default(),
            role: role_default(),
            database: database_default(),
            schema: None,
            warehouse: warehouse_default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    /// # The table types to include in the Python Model.
    #[serde(default = "default_table_types")]
    pub table_types: Vec<TableType>,

    /// # The output directory of the generated model.
    #[serde(default = "get_pwd")]
    pub output_dir: std::path::PathBuf,

    /// # The database configuration.
    #[serde(default)]
    pub database: indexmap::IndexMap<String, DatabaseConfig>,

    /// # The database names to exclude from the Python Model.
    #[serde(flatten, default)]
    pub database_pattern: Option<DatabasePattern>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DatabasePattern {
    /// # Specifies the database name to include in the Python Model.
    IncludeDatabases(Vec<String>),

    /// # Specifies the database name to exclude from the Python Model.
    ExcludeDatabases(Vec<String>),
}

impl Default for DatabasePattern {
    fn default() -> Self {
        DatabasePattern::ExcludeDatabases(vec![])
    }
}

impl ModelConfig {
    pub fn include_database(&self, database_name: &str) -> bool {
        match self
            .database_pattern
            .as_ref()
            .unwrap_or(&DatabasePattern::default())
        {
            DatabasePattern::IncludeDatabases(databases) => {
                databases.contains(&database_name.to_string())
            }
            DatabasePattern::ExcludeDatabases(databases) => {
                !databases.contains(&database_name.to_string())
            }
        }
    }

    pub fn include_database_schema(&self, database_name: &str, schema_name: &str) -> bool {
        if let Some(database_config) = self.database.get(database_name) {
            database_config.include_schema(schema_name)
        } else {
            DatabaseConfig::default().include_schema(schema_name)
        }
    }

    pub fn get_schema_table_types(&self, database_name: &str, schema_name: &str) -> &[TableType] {
        if let Some(database) = self.database.get(database_name) {
            if let Some(schema) = database.schema.get(schema_name) {
                if let Some(table_types) = &schema.table_types {
                    return table_types;
                }
            }
            if let Some(table_types) = &database.table_types {
                return table_types;
            }
        }
        &self.table_types
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, schemars::JsonSchema)]
pub struct DatabaseConfig {
    /// # The table types of database to include in the Python Model.
    pub table_types: Option<Vec<TableType>>,

    /// # The database configuration.
    #[serde(default)]
    pub schema: indexmap::IndexMap<String, SchemaConfig>,

    #[serde(flatten, default)]
    pub schema_pattern: Option<DatabaseSchemaPattern>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, schemars::JsonSchema)]
pub struct SchemaConfig {
    /// # The table types of schema to include in the Python Model.
    pub table_types: Option<Vec<TableType>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum TableType {
    #[serde(rename = "BASE TABLE")]
    BaseTable,
    View,
}

impl TryFrom<&str> for TableType {
    type Error = crate::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "BASE TABLE" => Ok(TableType::BaseTable),
            "VIEW" => Ok(TableType::View),
            _ => Err(crate::Error::InvalidTableType(value.into())),
        }
    }
}

impl std::fmt::Display for TableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableType::BaseTable => write!(f, "BASE TABLE"),
            TableType::View => write!(f, "VIEW"),
        }
    }
}

fn default_table_types() -> Vec<TableType> {
    vec![TableType::BaseTable, TableType::View]
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseSchemaPattern {
    /// # Specifies the schema name to include in the Python Model.
    IncludeSchemas(Vec<String>),

    /// # Specifies the schema name to exclude from the Python Model.
    ExcludeSchemas(Vec<String>),
}

impl DatabaseConfig {
    pub fn include_schema(&self, schema_name: &str) -> bool {
        self.schema_pattern
            .as_ref()
            .is_none_or(|schema_pattern| match schema_pattern {
                DatabaseSchemaPattern::IncludeSchemas(schemas) => {
                    schemas.contains(&schema_name.to_string())
                }
                DatabaseSchemaPattern::ExcludeSchemas(schemas) => {
                    !schemas.contains(&schema_name.to_string())
                }
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PydanticConfig {
    /// # The prefix of the model name.
    pub model_name_prefix: Option<String>,

    /// # The suffix of the model name.
    pub model_name_suffix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
/// # Represents a string or an environment variable.
pub enum StringOrEnv {
    String(String),
    Env(Env),
}

impl StringOrEnv {
    pub fn try_get_value(&self) -> Result<String, crate::Error> {
        match self {
            StringOrEnv::String(s) => Ok(s.clone()),
            StringOrEnv::Env(env) => {
                std::env::var(&env.env).map_err(|_| crate::Error::from_env_var(&env.env))
            }
        }
    }
}

pub fn new_env(env: &str) -> StringOrEnv {
    StringOrEnv::Env(Env {
        env: env.to_string(),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Env {
    /// # The environment variable name.
    env: String,
}

fn account_default() -> StringOrEnv {
    new_env("SNOWFLAKE_ACCOUNT")
}

fn user_default() -> StringOrEnv {
    new_env("SNOWFLAKE_USER")
}

fn password_default() -> StringOrEnv {
    new_env("SNOWFLAKE_PASSWORD")
}

fn private_key_default() -> StringOrEnv {
    new_env("SNOWFLAKE_PRIVATE_KEY")
}

fn private_key_path_default() -> StringOrEnv {
    new_env("SNOWFLAKE_PRIVATE_KEY_PATH")
}

fn private_key_passphrase_default() -> StringOrEnv {
    new_env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
}

fn role_default() -> StringOrEnv {
    new_env("SNOWFLAKE_ROLE")
}

fn database_default() -> StringOrEnv {
    new_env("SNOWFLAKE_DATABASE")
}

fn warehouse_default() -> StringOrEnv {
    new_env("SNOWFLAKE_WAREHOUSE")
}

pub fn find_path() -> Result<ConfigSource, crate::Error> {
    // find recursively from current directory to root directory
    let mut current_dir = std::env::current_dir().unwrap();
    loop {
        let config_path = current_dir.join(CONFIG_FILENAME);
        if config_path.exists() {
            log::debug!("\"{}\" found: {:?}", CONFIG_FILENAME, config_path);

            return Ok(ConfigSource::SnowmanToml(config_path));
        }

        let pyproject_toml_path = current_dir.join(PYPROJECT_FILENAME);
        if pyproject_toml_path.exists() {
            log::debug!(
                "\"{}\" found: {:?}",
                PYPROJECT_FILENAME,
                pyproject_toml_path
            );

            return Ok(ConfigSource::PyProjectToml(pyproject_toml_path));
        }

        if !current_dir.pop() {
            break;
        }
    }

    Err(Error::ConfigFileNotFound(CONFIG_FILENAME.into()))
}

pub fn load_from_source(source: &ConfigSource) -> Result<Config, crate::Error> {
    match source {
        ConfigSource::PyProjectToml(path) => {
            let pyproject_toml = std::fs::read_to_string(path)?;
            let pyproject_toml: PyProjectToml = toml::from_str(&pyproject_toml)?;

            if let PyProjectToml {
                tool: Some(Tool {
                    snowman: Some(config),
                }),
            } = pyproject_toml
            {
                Ok(config)
            } else {
                Err(Error::ConfigFileNotFound(CONFIG_FILENAME.into()))
            }
        }
        ConfigSource::SnowmanToml(path) => {
            let config = std::fs::read_to_string(path)?;
            toml::from_str(&config).map_err(Into::into)
        }
    }
}

pub fn load() -> Result<Config, crate::Error> {
    load_from_source(&find_path()?)
}

fn get_pwd() -> std::path::PathBuf {
    ".".into()
}

pub fn write_as_toml(config: &Config, filepath: &std::path::Path) -> Result<(), crate::Error> {
    let file = std::fs::File::create(filepath)?;
    let mut writer = std::io::BufWriter::new(file);
    let toml = toml::to_string(&config)?;
    writer.write_all(toml.as_bytes())?;

    Ok(())
}

pub fn create_file(filepath: &std::path::Path) -> Result<(), crate::Error> {
    if filepath.exists() {
        Err(crate::Error::ConfigFileAlreadyExists(
            filepath.to_path_buf(),
        ))?
    }

    std::fs::File::create(filepath)?
        .write_all(DEFAULT_CONFIG_STRING.as_bytes())
        .map_err(Into::into)
}

pub fn append_pyproject_tool(filepath: &std::path::Path) -> Result<(), crate::Error> {
    if !filepath.exists() {
        Err(crate::Error::ConfigFileNotFound(filepath.to_path_buf()))?
    }

    if let PyProjectToml {
        tool: Some(Tool {
            snowman: Some(_snowman),
        }),
    } = toml::from_str(&std::fs::read_to_string(filepath)?)?
    {
        Err(crate::Error::PyProjectTomlAlreadySet)?
    };

    let mut file = std::fs::OpenOptions::new().append(true).open(filepath)?;

    file.write_all(DEFAULT_PYPROJECT_TOML_STRING.as_bytes())?;

    Ok(())
}

#[doc(hidden)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
struct PyProjectToml {
    tool: Option<Tool>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
struct Tool {
    snowman: Option<Config>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub enum ConfigSource {
    /// "snowman.toml" filepath.
    SnowmanToml(std::path::PathBuf),

    /// "pyproject.toml" filepath.
    PyProjectToml(std::path::PathBuf),
}

impl AsRef<std::path::Path> for ConfigSource {
    fn as_ref(&self) -> &std::path::Path {
        match self {
            ConfigSource::SnowmanToml(path) => path.as_ref(),
            ConfigSource::PyProjectToml(path) => path.as_ref(),
        }
    }
}

const DEFAULT_CONFIG_STRING: &str = r#"# [Snowman](https://github.com/ya7010/snowman-py)
#:schema https://raw.githubusercontent.com/ya7010/snowman-py/main/docs/snowman.schema.json

[connection]
account = { env = "SNOWFLAKE_ACCOUNT" }
user = { env = "SNOWFLAKE_USER" }
password = { env = "SNOWFLAKE_PASSWORD" }
role = { env = "SNOWFLAKE_ROLE" }
database = { env = "SNOWFLAKE_DATABASE" }
warehouse = { env = "SNOWFLAKE_WAREHOUSE" }

[model]
output_dir = "."
exclude_databases = ["INFORMATION_SCHEMA"]

[pydantic]
# model_name_suffix = "Model"
"#;

const DEFAULT_PYPROJECT_TOML_STRING: &str = r#"
[tool.snowman.model]
output_dir = "."
exclude_databases = ["INFORMATION_SCHEMA"]

[tool.snowman.pydantic]
"#;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_default_config_string() {
        toml::from_str::<Config>(super::DEFAULT_CONFIG_STRING).unwrap();
    }

    #[test]
    fn test_parse_default_pyproject_toml_string() {
        toml::from_str::<PyProjectToml>(super::DEFAULT_PYPROJECT_TOML_STRING).unwrap();
    }

    #[test]
    fn test_database_config_default_include_schema() {
        assert!(DatabaseConfig::default().include_schema("SCHEMA"));
    }

    #[test]
    fn test_database_config_default_exclude_schema() {
        assert!(!DatabaseConfig {
            schema_pattern: Some(DatabaseSchemaPattern::ExcludeSchemas(vec![
                "INFORMATION_SCHEMA".to_string()
            ])),
            ..Default::default()
        }
        .include_schema("INFORMATION_SCHEMA"));
    }

    #[test]
    fn test_database_config_exclude_schema() {
        let database_config = DatabaseConfig {
            schema_pattern: Some(DatabaseSchemaPattern::ExcludeSchemas(vec![
                "INFORMATION_SCHEMA".to_string(),
            ])),
            ..Default::default()
        };

        assert!(database_config.include_schema("SCHEMA"));
    }

    #[test]
    fn test_table_type_to_string() {
        let table_type = TableType::BaseTable;
        assert_eq!(table_type.to_string(), "BASE TABLE");

        let table_type = TableType::View;
        assert_eq!(table_type.to_string(), "VIEW");
    }

    #[test]
    fn test_table_type_from_string() {
        assert_eq!(
            TableType::try_from("BASE TABLE").ok(),
            Some(TableType::BaseTable)
        );
        assert_eq!(TableType::try_from("VIEW").ok(), Some(TableType::View));
    }

    #[test]
    fn test_table_type_serialize() {
        let table_type = TableType::BaseTable;
        assert_eq!(
            serde_json::to_string(&table_type).unwrap(),
            r#""BASE TABLE""#
        );

        let table_type = TableType::View;
        assert_eq!(serde_json::to_string(&table_type).unwrap(), r#""VIEW""#);
    }

    #[test]
    fn test_table_type_deserialize() {
        assert_eq!(
            serde_json::from_str::<TableType>(r#""BASE TABLE""#).unwrap(),
            TableType::BaseTable
        );

        assert_eq!(
            serde_json::from_str::<TableType>(r#""VIEW""#).unwrap(),
            TableType::View
        );
    }
}
