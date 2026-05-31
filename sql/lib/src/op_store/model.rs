use std::collections::BTreeMap;

use jj_sql_macro::sql;
use sqlx::Row as _;
use sqlx::Sqlite;
use sqlx::sqlite::SqliteArguments;
use sqlx::sqlite::SqliteRow;

use crate::backend::model::CommitId;
use crate::model::Model;
use crate::id_newtype;
use crate::postcard::Postcard;
use crate::row_id_newtype;
use crate::str_newtype;

pub const VIEW_ID_LENGTH: usize = 64;
pub const OPERATION_ID_LENGTH: usize = 64;

row_id_newtype!(ViewRowId);
row_id_newtype!(OperationRowId);

id_newtype!(ViewId, VIEW_ID_LENGTH, true);
id_newtype!(OperationId, OPERATION_ID_LENGTH, true);

str_newtype!(RefName);
str_newtype!(RemoteName);
str_newtype!(GitRefName);
str_newtype!(WorkspaceName);

pub type CommitPredecessors = BTreeMap<CommitId, Vec<CommitId>>;

#[derive(
    Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, proptest_derive::Arbitrary,
)]
pub struct Timestamp {
    pub ms: i64,
    pub tz: i32,
}

#[derive(
    Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, proptest_derive::Arbitrary,
)]
pub struct RefTarget {
    /// Merge terms in flat vec order: [add0, remove0, add1, remove1, …].
    pub terms: Vec<Option<CommitId>>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, proptest_derive::Arbitrary,
)]
pub struct RemoteRef {
    pub target: RefTarget,
    pub tracked: bool,
}

#[derive(
    Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, proptest_derive::Arbitrary,
)]
pub struct RemoteView {
    pub bookmarks: BTreeMap<RefName, RemoteRef>,
    pub tags: BTreeMap<RefName, RemoteRef>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, proptest_derive::Arbitrary,
)]
pub struct View {
    pub head_ids: Vec<CommitId>,
    pub local_bookmarks: BTreeMap<RefName, RefTarget>,
    pub local_tags: BTreeMap<RefName, RefTarget>,
    pub remote_views: BTreeMap<RemoteName, RemoteView>,
    pub git_refs: BTreeMap<GitRefName, RefTarget>,
    pub git_head: RefTarget,
    pub wc_commit_ids: BTreeMap<WorkspaceName, CommitId>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, proptest_derive::Arbitrary,
)]
pub struct OperationMetadata {
    pub time_start: Timestamp,
    pub time_end: Timestamp,
    pub description: String,
    pub hostname: String,
    pub username: String,
    pub is_snapshot: bool,
    pub workspace_name: Option<String>,
    pub attributes: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow, proptest_derive::Arbitrary)]
pub struct ViewRow {
    pub id: ViewId,
    pub data: Postcard<View>,
}

#[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow, proptest_derive::Arbitrary)]
pub struct OperationRow {
    pub id: OperationId,
    pub view_id: ViewId,
    pub parents: Postcard<Vec<OperationId>>,
    pub metadata: Postcard<OperationMetadata>,
    pub commit_predecessors: Option<Postcard<CommitPredecessors>>,
}

impl Model for ViewRow {
    const TABLE_NAME: &str = "views";

    type RowId = ViewRowId;
    type HashId = ViewId;

    fn hash_id(&self) -> &ViewId {
        &self.id
    }

    fn insert_query(&self) -> sqlx::query::Query<'_, Sqlite, SqliteArguments> {
        sqlx::query!(
            "
            INSERT INTO views (id, data, __last_written_ms)
            VALUES (?, ?, unixepoch('now') * 1000)
            ON CONFLICT(id) DO UPDATE
            SET __last_written_ms = MAX(__last_written_ms, excluded.__last_written_ms)
            ",
            self.id,
            self.data,
        )
    }

    fn fetch_by_row_id_query(
        row_id: ViewRowId,
    ) -> sqlx::query::Map<
        'static,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<Self> + Send,
        SqliteArguments,
    > {
        sqlx::query(sql!("SELECT id, data FROM views WHERE row_id = ?"))
            .bind(row_id)
            .try_map(|row| sqlx::FromRow::<SqliteRow>::from_row(&row))
    }

    fn fetch_by_hash_id_query(
        hash_id: &ViewId,
    ) -> sqlx::query::Map<
        '_,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<(ViewRowId, Self)> + Send,
        SqliteArguments,
    > {
        sqlx::query(sql!("SELECT row_id, id, data FROM views WHERE id = ?"))
            .bind(hash_id)
            .try_map(|row| {
                Ok((
                    row.try_get("row_id")?,
                    sqlx::FromRow::<SqliteRow>::from_row(&row)?,
                ))
            })
    }
}

impl Model for OperationRow {
    const TABLE_NAME: &str = "operations";

    type RowId = OperationRowId;
    type HashId = OperationId;

    fn hash_id(&self) -> &OperationId {
        &self.id
    }

    fn insert_query(&self) -> sqlx::query::Query<'_, Sqlite, SqliteArguments> {
        sqlx::query!(
            "
            INSERT INTO operations (
                id,
                view_id,
                parents,
                metadata,
                commit_predecessors,
                __last_written_ms
            )
            VALUES (?, ?, ?, ?, ?, unixepoch('now') * 1000)
            ON CONFLICT(id) DO UPDATE
            SET __last_written_ms = MAX(__last_written_ms, excluded.__last_written_ms)
            ",
            self.id,
            self.view_id,
            self.parents,
            self.metadata,
            self.commit_predecessors,
        )
    }

    fn fetch_by_row_id_query(
        row_id: OperationRowId,
    ) -> sqlx::query::Map<
        'static,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<Self> + Send,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT id, view_id, parents, metadata, commit_predecessors
            FROM operations
            WHERE row_id = ?
            "
        ))
        .bind(row_id)
        .try_map(|row| sqlx::FromRow::<SqliteRow>::from_row(&row))
    }

    fn fetch_by_hash_id_query(
        hash_id: &OperationId,
    ) -> sqlx::query::Map<
        '_,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<(OperationRowId, Self)> + Send,
        SqliteArguments,
    > {
        sqlx::query(sql!(
            "
            SELECT row_id, id, view_id, parents, metadata, commit_predecessors
            FROM operations
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
    use crate::model::test_model_roundtrip;
    use crate::model::test_model_roundtrip_with;
    use crate::op_store::SqlOpStore;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_roundtrip() -> anyhow::Result<()> {
        let store = SqlOpStore::init_in_memory().await?;
        // ViewRow wraps a deeply-nested View (maps of maps of vecs); 16 cases are sufficient to
        // catch encoding bugs without running for minutes.
        test_model_roundtrip_with::<ViewRow>(
            &store.pool,
            proptest::test_runner::Config { cases: 16, ..Default::default() },
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_operation_roundtrip() -> anyhow::Result<()> {
        let store = SqlOpStore::init_in_memory().await?;
        test_model_roundtrip::<OperationRow>(&store.pool).await
    }
}
