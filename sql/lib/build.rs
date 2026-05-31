use sqlx::ConnectOptions as _;

const SCHEMAS: &[&str] = &[
    "CREATE TABLE unpack_i64s (val INTEGER NOT NULL, blob BLOB NOT NULL);",
    "CREATE TABLE unpack_postcard_i64s (val INTEGER NOT NULL, blob BLOB NOT NULL);",
    "CREATE TABLE unpack_blobs (val BLOB NOT NULL, data BLOB NOT NULL, stride INTEGER NOT NULL);",
    include_str!("sql/backend.sql"),
    include_str!("sql/op_heads.sql"),
    include_str!("sql/op_store.sql"),
];

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed=schema.sql");

    let out_dir = std::env::var("OUT_DIR")?;
    let db_path = std::path::Path::new(&out_dir).join("sqlx-check.db");
    let url = format!("sqlite://{}", db_path.display());

    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }

    let mut conn = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true)
        .connect()
        .await?;
    for schema in SCHEMAS {
        sqlx::raw_sql(*schema).execute(&mut conn).await?;
    }

    println!("cargo:rustc-env=DATABASE_URL={url}");
    println!("cargo:rustc-env=SQL_SCHEMA_DB={}", db_path.display());
    Ok(())
}
