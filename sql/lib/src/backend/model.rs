use std::io::Read;

use jj_sql_macro::sql;
use sqlx::Row as _;
use sqlx::Sqlite;
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteArguments;
use sqlx::sqlite::SqliteRow;

use crate::SimHash;
use crate::SqlBackendError;
use crate::error::SqlBackendResult;
use crate::id_newtype;
pub use crate::model::Model;
pub use crate::model::SqliteConnectionExt;
use crate::postcard::Postcard;
use crate::row_id_newtype;

pub const COMMIT_ID_LENGTH: usize = 64;
pub const CHANGE_ID_LENGTH: usize = 16;
pub const SIMHASH_WINDOW_SIZE: usize = 8;

row_id_newtype!(FileRowId);
row_id_newtype!(SymlinkRowId);
row_id_newtype!(TreeRowId);
row_id_newtype!(CommitRowId);
row_id_newtype!(CopyHistoryRowId);

id_newtype!(FileId, COMMIT_ID_LENGTH, true);
id_newtype!(SymlinkId, COMMIT_ID_LENGTH, true);
id_newtype!(TreeId, COMMIT_ID_LENGTH, true);
id_newtype!(CommitId, COMMIT_ID_LENGTH, true);
id_newtype!(CopyId, COMMIT_ID_LENGTH, true);

id_newtype!(ChangeId, CHANGE_ID_LENGTH, false);

#[derive(Clone, Debug, PartialEq, Eq, proptest_derive::Arbitrary)]
pub struct Signature {
    pub name: String,
    pub email: String,
    pub timestamp: i64,
    pub tz_offset: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, proptest_derive::Arbitrary)]
pub struct SecureSig {
    pub data: Vec<u8>,
    pub sig: Vec<u8>,
}

// NOTE: We deliberately do not use an enum for `CompressionMode` since we want
// an _open_ enum to make the file metadata usable even for versions that don't
// support all the used compression modes.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    sqlx::Type,
    proptest_derive::Arbitrary,
)]
#[repr(transparent)]
#[sqlx(transparent)]
pub struct CompressionMode(pub(crate) u8);

#[derive(Clone, Debug, PartialEq, Eq, proptest_derive::Arbitrary, sqlx::FromRow)]
pub struct File {
    /// Hash of the _uncompressed_ content.
    pub id: FileId,
    /// Size of the _uncompressed_ content.
    pub size: i64,
    /// Similarity hash of the _uncompressed_ content.
    pub simhash: Option<SimHash<SIMHASH_WINDOW_SIZE>>,
    /// Mode used for compressing the content.
    pub compression_mode: CompressionMode,
    /// ID of the base object used for compression. The exact meaning
    /// depends on the value of `compression_mode`.
    pub compression_base_id: Option<i64>,
    /// The _compressed_ content.
    pub compressed_data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, proptest_derive::Arbitrary, sqlx::FromRow)]
pub struct Symlink {
    pub id: SymlinkId,
    pub target: String,
}

#[derive(
    Clone, Debug, PartialEq, Eq, proptest_derive::Arbitrary, serde::Serialize, serde::Deserialize,
)]
pub enum TreeValue {
    File {
        row_id: FileRowId,
        executable: bool,
        // TODO: Use a `CopyRowId` here.
        copy_id: Option<CopyId>,
    },
    Symlink(SymlinkRowId),
    Tree(TreeRowId),
    Submodule(CommitRowId),
}

pub type TreeEntries = Vec<(String, TreeValue)>;

#[derive(Clone, Debug, PartialEq, Eq, proptest_derive::Arbitrary, sqlx::FromRow)]
pub struct Tree {
    pub id: TreeId,
    pub entries: Postcard<TreeEntries>,
}

#[derive(Clone, Debug, PartialEq, Eq, proptest_derive::Arbitrary)]
pub struct Commit {
    pub id: CommitId,
    pub parents: Postcard<Vec<CommitRowId>>,
    pub predecessors: Postcard<Vec<CommitRowId>>,
    pub root_trees: Postcard<Vec<TreeRowId>>,
    pub conflict_labels: Postcard<Vec<String>>,
    pub change_id: ChangeId,
    pub description: String,
    pub author: Signature,
    pub committer: Signature,
    pub secure_sig: Option<SecureSig>,
}

#[derive(Clone, Debug, PartialEq, Eq, proptest_derive::Arbitrary, sqlx::FromRow)]
pub struct CopyHistory {
    pub id: CopyId,
    pub generation: i64,
    pub current_path: String,
    pub parents: Postcard<Vec<CopyHistoryRowId>>,
    pub salt: Vec<u8>,
}

impl CompressionMode {
    /// No compresseion at all (aka "compression" with the identity function).
    pub const NONE: Self = Self(0);
    /// Zstandard compression without any dictionary.
    pub const ZSTD: Self = Self(1);
    /// Zstandard compression using the file referenced by `compression_base_id`
    /// as dictionary.
    pub const ZSTD_SIMILAR: Self = Self(2);
}

impl Model for File {
    const TABLE_NAME: &str = "files";

    type RowId = FileRowId;
    type HashId = FileId;

    fn hash_id(&self) -> &Self::HashId {
        &self.id
    }

    fn insert_query(&self) -> sqlx::query::Query<'_, Sqlite, SqliteArguments> {
        sqlx::query!(
            "
            INSERT INTO files (
                id,
                size,
                simhash,
                compression_mode,
                compression_base_id,
                compressed_data,
                __last_written_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, unixepoch('now') * 1000)
            ON CONFLICT(id) DO UPDATE
            SET __last_written_ms = MAX(__last_written_ms, excluded.__last_written_ms)
            ",
            self.id,
            self.size,
            self.simhash,
            self.compression_mode,
            self.compression_base_id,
            self.compressed_data,
        )
    }

    fn fetch_by_row_id_query(
        row_id: Self::RowId,
    ) -> sqlx::query::Map<
        'static,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<Self> + Send,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT
                id,
                size,
                simhash,
                compression_mode,
                compression_base_id,
                compressed_data
            FROM files
            WHERE row_id = ?
            "
        ))
        .bind(row_id)
        .try_map(|row| sqlx::FromRow::<SqliteRow>::from_row(&row))
    }

    fn fetch_by_hash_id_query(
        hash_id: &Self::HashId,
    ) -> sqlx::query::Map<
        '_,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<(Self::RowId, Self)> + Send,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT
                row_id,
                id,
                size,
                simhash,
                compression_mode,
                compression_base_id,
                compressed_data
            FROM files
            WHERE id = ?
            "
        ))
        .bind(hash_id)
        .try_map(|row| {
            Ok((
                row.try_get("row_id")?,
                sqlx::FromRow::<SqliteRow>::from_row(&row)?,
            ))
        })
    }
}

impl File {
    pub async fn decompress(self, pool: &SqlitePool) -> SqlBackendResult<Vec<u8>> {
        // TODO: See if can use blob I/O to avoid allocating the vector for the
        // compressed data.
        match self.compression_mode {
            CompressionMode::NONE => {
                debug_assert!(self.compression_base_id.is_none());
                Ok(self.compressed_data)
            }
            CompressionMode::ZSTD => {
                debug_assert!(self.compression_base_id.is_none());
                let content = zstd::decode_all(self.compressed_data.as_slice())?;
                Ok(content)
            }
            CompressionMode::ZSTD_SIMILAR => {
                let Some(base_id) = self.compression_base_id else {
                    return Err(SqlBackendError::InternalError(String::from(
                        "ZSTD_SIMILAR compressed file without base ID.",
                    )));
                };
                let base_file = pool
                    .acquire()
                    .await?
                    .fetch_by_row_id::<File>(FileRowId(base_id))
                    .await?;
                // TODO: Guard this against unbounded recursion or turn into a bounded loop.
                let base_content = Box::pin(base_file.decompress(pool)).await?;
                let mut decompressor = zstd::Decoder::with_dictionary(
                    self.compressed_data.as_slice(),
                    base_content.as_slice(),
                )?;
                let mut content = Vec::new();
                decompressor.read_to_end(&mut content)?;
                Ok(content)
            }
            other => Err(SqlBackendError::InternalError(format!(
                "Unsupported compression mode: {}",
                other.0,
            ))),
        }
    }
}

impl Model for Symlink {
    const TABLE_NAME: &str = "symlinks";

    type RowId = SymlinkRowId;
    type HashId = SymlinkId;

    fn hash_id(&self) -> &Self::HashId {
        &self.id
    }

    fn insert_query(&self) -> sqlx::query::Query<'_, Sqlite, SqliteArguments> {
        sqlx::query!(
            "
            INSERT INTO symlinks (
                id,
                target,
                __last_written_ms
            )
            VALUES (?, ?, unixepoch('now') * 1000)
            ON CONFLICT(id) DO UPDATE
            SET __last_written_ms = MAX(__last_written_ms, excluded.__last_written_ms)
            ",
            self.id,
            self.target,
        )
    }

    fn fetch_by_row_id_query(
        row_id: Self::RowId,
    ) -> sqlx::query::Map<
        'static,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<Self>,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT id, target
            FROM symlinks
            WHERE row_id = ?
            "
        ))
        .bind(row_id)
        .try_map(|row| sqlx::FromRow::<SqliteRow>::from_row(&row))
    }

    fn fetch_by_hash_id_query(
        hash_id: &Self::HashId,
    ) -> sqlx::query::Map<
        '_,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<(Self::RowId, Self)>,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT row_id, id, target
            FROM symlinks
            WHERE id = ?
            "
        ))
        .bind(hash_id)
        .try_map(|row| {
            Ok((
                row.try_get("row_id")?,
                sqlx::FromRow::<SqliteRow>::from_row(&row)?,
            ))
        })
    }
}

impl Model for Tree {
    const TABLE_NAME: &str = "trees";

    type RowId = TreeRowId;
    type HashId = TreeId;

    fn hash_id(&self) -> &Self::HashId {
        &self.id
    }

    fn insert_query(&self) -> sqlx::query::Query<'_, Sqlite, SqliteArguments> {
        sqlx::query!(
            "
            INSERT INTO trees (
                id,
                entries,
                __last_written_ms
            )
            VALUES (?, ?, unixepoch('now') * 1000)
            ON CONFLICT(id) DO UPDATE
            SET __last_written_ms = MAX(__last_written_ms, excluded.__last_written_ms)
            ",
            self.id,
            self.entries,
        )
    }

    fn fetch_by_row_id_query(
        row_id: Self::RowId,
    ) -> sqlx::query::Map<
        'static,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<Self> + Send,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT id, entries
            FROM trees
            WHERE row_id = ?
            "
        ))
        .bind(row_id)
        .try_map(|row| sqlx::FromRow::<SqliteRow>::from_row(&row))
    }

    fn fetch_by_hash_id_query(
        hash_id: &Self::HashId,
    ) -> sqlx::query::Map<
        '_,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<(Self::RowId, Self)> + Send,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT row_id, id, entries
            FROM trees
            WHERE id = ?
            "
        ))
        .bind(hash_id)
        .try_map(|row| {
            Ok((
                row.try_get("row_id")?,
                sqlx::FromRow::<SqliteRow>::from_row(&row)?,
            ))
        })
    }
}

#[derive(sqlx::FromRow)]
struct CommitRaw {
    id: CommitId,
    parents: Postcard<Vec<CommitRowId>>,
    predecessors: Postcard<Vec<CommitRowId>>,
    root_trees: Postcard<Vec<TreeRowId>>,
    conflict_labels: Postcard<Vec<String>>,
    change_id: ChangeId,
    description: String,
    author_name: String,
    author_email: String,
    author_timestamp: i64,
    author_tz_offset: i32,
    committer_name: String,
    committer_email: String,
    committer_timestamp: i64,
    committer_tz_offset: i32,
    secure_sig_data: Option<Vec<u8>>,
    secure_sig_sig: Option<Vec<u8>>,
}

impl TryFrom<CommitRaw> for Commit {
    type Error = sqlx::Error;

    fn try_from(raw: CommitRaw) -> Result<Self, Self::Error> {
        let secure_sig = match (raw.secure_sig_data, raw.secure_sig_sig) {
            (Some(data), Some(sig)) => Some(SecureSig { data, sig }),
            (None, None) => None,
            _ => {
                return Err(sqlx::Error::ColumnDecode {
                    index: "secure_sig".to_string(),
                    source: "inconsistent NULL values for secure_sig columns".into(),
                });
            }
        };
        Ok(Commit {
            id: raw.id,
            parents: raw.parents,
            predecessors: raw.predecessors,
            root_trees: raw.root_trees,
            conflict_labels: raw.conflict_labels,
            change_id: raw.change_id,
            description: raw.description,
            author: Signature {
                name: raw.author_name,
                email: raw.author_email,
                timestamp: raw.author_timestamp,
                tz_offset: raw.author_tz_offset,
            },
            committer: Signature {
                name: raw.committer_name,
                email: raw.committer_email,
                timestamp: raw.committer_timestamp,
                tz_offset: raw.committer_tz_offset,
            },
            secure_sig,
        })
    }
}

impl Model for Commit {
    const TABLE_NAME: &str = "commits";

    type RowId = CommitRowId;
    type HashId = CommitId;

    fn hash_id(&self) -> &Self::HashId {
        &self.id
    }

    fn insert_query(&self) -> sqlx::query::Query<'_, Sqlite, SqliteArguments> {
        sqlx::query!(
            "
            INSERT INTO commits (
                id,
                parents,
                predecessors,
                root_trees,
                conflict_labels,
                change_id,
                description,
                author_name,
                author_email,
                author_timestamp,
                author_tz_offset,
                committer_name,
                committer_email,
                committer_timestamp,
                committer_tz_offset,
                secure_sig_data,
                secure_sig_sig,
                __last_written_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch('now') * 1000)
            ON CONFLICT(id) DO UPDATE
            SET __last_written_ms = MAX(__last_written_ms, excluded.__last_written_ms)
            ",
            self.id,
            self.parents,
            self.predecessors,
            self.root_trees,
            self.conflict_labels,
            self.change_id,
            self.description,
            self.author.name,
            self.author.email,
            self.author.timestamp,
            self.author.tz_offset,
            self.committer.name,
            self.committer.email,
            self.committer.timestamp,
            self.committer.tz_offset,
            self.secure_sig.as_ref().map(|s| &s.data),
            self.secure_sig.as_ref().map(|s| &s.sig),
        )
    }

    fn fetch_by_row_id_query(
        row_id: Self::RowId,
    ) -> sqlx::query::Map<
        'static,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<Self> + Send,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT
                id,
                parents,
                predecessors,
                root_trees,
                conflict_labels,
                change_id,
                description,
                author_name,
                author_email,
                author_timestamp,
                author_tz_offset,
                committer_name,
                committer_email,
                committer_timestamp,
                committer_tz_offset,
                secure_sig_data,
                secure_sig_sig
            FROM commits
            WHERE row_id = ?
            "
        ))
        .bind(row_id)
        .try_map(|row| <CommitRaw as sqlx::FromRow<SqliteRow>>::from_row(&row)?.try_into())
    }

    fn fetch_by_hash_id_query(
        hash_id: &Self::HashId,
    ) -> sqlx::query::Map<
        '_,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<(Self::RowId, Self)> + Send,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT
                row_id,
                id,
                parents,
                predecessors,
                root_trees,
                conflict_labels,
                change_id,
                description,
                author_name,
                author_email,
                author_timestamp,
                author_tz_offset,
                committer_name,
                committer_email,
                committer_timestamp,
                committer_tz_offset,
                secure_sig_data,
                secure_sig_sig
            FROM commits
            WHERE id = ?
            "
        ))
        .bind(hash_id)
        .try_map(|row| {
            Ok((
                row.try_get("row_id")?,
                <CommitRaw as sqlx::FromRow<SqliteRow>>::from_row(&row)?.try_into()?,
            ))
        })
    }
}

impl Model for CopyHistory {
    const TABLE_NAME: &str = "copies";

    type RowId = CopyHistoryRowId;
    type HashId = CopyId;

    fn hash_id(&self) -> &Self::HashId {
        &self.id
    }

    fn insert_query(&self) -> sqlx::query::Query<'_, Sqlite, SqliteArguments> {
        sqlx::query!(
            "
            INSERT INTO copies (
                id,
                generation,
                current_path,
                parents,
                salt,
                __last_written_ms
            )
            VALUES (?, ?, ?, ?, ?, unixepoch('now') * 1000)
            ON CONFLICT(id) DO UPDATE
            SET __last_written_ms = MAX(__last_written_ms, excluded.__last_written_ms)
            ",
            self.id,
            self.generation,
            self.current_path,
            self.parents,
            self.salt,
        )
    }

    fn fetch_by_row_id_query(
        row_id: Self::RowId,
    ) -> sqlx::query::Map<
        'static,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<Self>,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT id, generation, current_path, parents, salt
            FROM copies
            WHERE row_id = ?
            "
        ))
        .bind(row_id)
        .try_map(|row| sqlx::FromRow::<SqliteRow>::from_row(&row))
    }

    fn fetch_by_hash_id_query(
        hash_id: &Self::HashId,
    ) -> sqlx::query::Map<
        '_,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<(Self::RowId, Self)>,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT row_id, id, generation, current_path, parents, salt
            FROM copies
            WHERE id = ?
            "
        ))
        .bind(hash_id)
        .try_map(|row| {
            Ok((
                row.try_get("row_id")?,
                sqlx::FromRow::<SqliteRow>::from_row(&row)?,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqlBackend;
    use crate::model::test_model_roundtrip;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_file_roundtrip() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        test_model_roundtrip::<File>(&backend.pool).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_symlink_roundtrip() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        test_model_roundtrip::<Symlink>(&backend.pool).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tree_roundtrip() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        test_model_roundtrip::<Tree>(&backend.pool).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_roundtrip() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        test_model_roundtrip::<Commit>(&backend.pool).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_copy_roundtrip() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        test_model_roundtrip::<CopyHistory>(&backend.pool).await
    }
}
