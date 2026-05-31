use proc_macro::TokenStream;
use quote::quote;
use syn::LitStr;
use syn::parse_macro_input;

fn verify_query(db_path: &str, sql: &str) -> Result<(), String> {
    let conn = rusqlite::Connection::open(db_path)
        .map_err(|e| format!("cannot open schema db '{db_path}': {e}"))?;
    conn.prepare(sql).map(|_| ()).map_err(|e| e.to_string())
}

/// Validates a SQL string literal against the schema database at compile time.
///
/// The path to the schema database is read from the `SQL_SCHEMA_DB` environment
/// variable. The macro expands to the original string literal on success, so it
/// has type `&'static str` and is a zero-cost abstraction at runtime.
#[proc_macro]
pub fn sql(input: TokenStream) -> TokenStream {
    let lit = parse_macro_input!(input as LitStr);
    let sql_text = lit.value();

    let Ok(db_path) = std::env::var("SQL_SCHEMA_DB") else {
        return quote! { compile_error!("SQL_SCHEMA_DB is not set") }.into();
    };

    match verify_query(&db_path, &sql_text) {
        Ok(()) => quote! { #lit }.into(),
        Err(msg) => {
            let msg = format!("sql!: {msg}");
            quote! { compile_error!(#msg) }.into()
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use tempfile::NamedTempFile;

    use super::*;

    fn make_db() -> NamedTempFile {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
             CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT);",
        )
        .unwrap();
        f
    }

    #[test]
    fn valid_select_passes() {
        let db = make_db();
        assert!(verify_query(db.path().to_str().unwrap(), "SELECT id, name FROM users").is_ok());
    }

    #[test]
    fn valid_placeholder_passes() {
        let db = make_db();
        assert!(
            verify_query(
                db.path().to_str().unwrap(),
                "SELECT id FROM users WHERE name = ?"
            )
            .is_ok()
        );
    }

    #[test]
    fn valid_join_passes() {
        let db = make_db();
        assert!(
            verify_query(
                db.path().to_str().unwrap(),
                "SELECT u.name, p.body FROM users u JOIN posts p ON p.user_id = u.id"
            )
            .is_ok()
        );
    }

    #[test]
    fn invalid_table_fails() {
        let db = make_db();
        assert!(verify_query(db.path().to_str().unwrap(), "SELECT id FROM ghosts").is_err());
    }

    #[test]
    fn invalid_column_fails() {
        let db = make_db();
        assert!(verify_query(db.path().to_str().unwrap(), "SELECT ghost FROM users").is_err());
    }

    #[test]
    fn missing_db_returns_error() {
        assert!(verify_query("/nonexistent/path/schema.db", "SELECT 1").is_err());
    }
}
