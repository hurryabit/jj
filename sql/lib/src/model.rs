use std::collections::HashMap;

use sqlx::AssertSqlSafe;
use sqlx::Sqlite;
use sqlx::sqlite::SqliteArguments;
use sqlx::sqlite::SqliteRow;
use zerocopy::IntoBytes;

use crate::Id;
use crate::error::SqlBackendResult;
use crate::hash::Hash;

pub const HASH_ID_LENGTH: usize = 64;

pub trait Model: Clone + std::fmt::Debug + Eq + Send + Sync + Unpin + 'static {
    const TABLE_NAME: &str;

    type RowId: Id<i64>;
    type HashId: Id<Hash<HASH_ID_LENGTH>>;

    fn hash_id(&self) -> &Self::HashId;

    fn insert_query(&self) -> sqlx::query::Query<'_, Sqlite, SqliteArguments>;

    fn fetch_by_row_id_query(
        row_id: Self::RowId,
    ) -> sqlx::query::Map<
        'static,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<Self> + Send,
        SqliteArguments,
    >;

    #[allow(clippy::type_complexity)]
    fn fetch_by_hash_id_query(
        hash_id: &Self::HashId,
    ) -> sqlx::query::Map<
        '_,
        Sqlite,
        impl FnMut(SqliteRow) -> sqlx::Result<(Self::RowId, Self)> + Send,
        SqliteArguments,
    >;
}

#[allow(async_fn_in_trait)]
pub trait SqliteConnectionExt {
    async fn insert<M: Model>(&mut self, model: &M) -> SqlBackendResult<M::RowId>;

    async fn fetch_by_row_id<M: Model>(&mut self, row_id: M::RowId) -> SqlBackendResult<M>;

    async fn fetch_by_hash_id<M: Model>(
        &mut self,
        hash_id: &M::HashId,
    ) -> SqlBackendResult<(M::RowId, M)>;

    async fn lookup_hash_ids<M: Model>(
        &mut self,
        row_ids: &[M::RowId],
    ) -> SqlBackendResult<Vec<M::HashId>>;

    async fn lookup_row_ids<M: Model>(
        &mut self,
        hash_ids: &[M::HashId],
    ) -> SqlBackendResult<Vec<M::RowId>>;
}

#[derive(sqlx::FromRow)]
struct IdPair {
    row_id: i64,
    id: Hash<HASH_ID_LENGTH>,
}

impl SqliteConnectionExt for sqlx::SqliteConnection {
    async fn insert<M: Model>(&mut self, model: &M) -> SqlBackendResult<M::RowId> {
        let result = model.insert_query().execute(self).await?;
        Ok(result.last_insert_rowid().into())
    }

    async fn fetch_by_row_id<M: Model>(&mut self, row_id: M::RowId) -> SqlBackendResult<M> {
        Ok(M::fetch_by_row_id_query(row_id).fetch_one(self).await?)
    }

    async fn fetch_by_hash_id<M: Model>(
        &mut self,
        hash_id: &M::HashId,
    ) -> SqlBackendResult<(M::RowId, M)> {
        Ok(M::fetch_by_hash_id_query(hash_id).fetch_one(self).await?)
    }

    async fn lookup_hash_ids<M: Model>(
        &mut self,
        row_ids: &[M::RowId],
    ) -> SqlBackendResult<Vec<M::HashId>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        // TODO: Avoid the intermediate vector.
        let rows: Vec<IdPair> = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT row_id, id FROM {} WHERE row_id IN (SELECT val FROM unpack_i64s WHERE blob = ?)",
            M::TABLE_NAME
        )))
        .bind(row_ids.as_bytes())
        .fetch_all(self)
        .await?;
        let map: HashMap<M::RowId, M::HashId> = rows
            .into_iter()
            .map(|p| (p.row_id.into(), p.id.into()))
            .collect();
        // TODO: Don't panic when some row_ids don't exist.
        Ok(row_ids.iter().map(|row_id| map[row_id]).collect())
    }

    async fn lookup_row_ids<M: Model>(
        &mut self,
        hash_ids: &[M::HashId],
    ) -> SqlBackendResult<Vec<M::RowId>> {
        if hash_ids.is_empty() {
            return Ok(Vec::new());
        }
        // TODO: Avoid the intermediate vector.
        let rows: Vec<IdPair> = sqlx::query_as(AssertSqlSafe(format!(
            "
            SELECT row_id, id
            FROM {}
            WHERE id IN (SELECT val FROM unpack_blobs WHERE data = ? AND stride = {})
            ",
            M::TABLE_NAME,
            HASH_ID_LENGTH,
        )))
        .bind(hash_ids.as_bytes())
        .fetch_all(self)
        .await?;
        let map: HashMap<M::HashId, M::RowId> = rows
            .into_iter()
            .map(|p| (p.id.into(), p.row_id.into()))
            .collect();
        // TODO: Don't panic when some row_ids don't exist.
        Ok(hash_ids.iter().map(|id| map[id]).collect())
    }
}

#[cfg(test)]
pub(crate) trait TestRunnerExt {
    fn run_async<S: proptest::strategy::Strategy>(
        &mut self,
        strategy: &S,
        test: impl AsyncFn(S::Value) -> proptest::test_runner::TestCaseResult,
    ) -> Result<(), proptest::test_runner::TestError<S::Value>>;
}

#[cfg(test)]
impl TestRunnerExt for proptest::test_runner::TestRunner {
    fn run_async<S: proptest::strategy::Strategy>(
        &mut self,
        strategy: &S,
        test: impl AsyncFn(S::Value) -> proptest::test_runner::TestCaseResult,
    ) -> Result<(), proptest::test_runner::TestError<S::Value>> {
        let handle = tokio::runtime::Handle::current();
        self.run(strategy, |value| {
            tokio::task::block_in_place(|| handle.block_on(test(value)))
        })
    }
}

#[cfg(test)]
pub(crate) async fn test_model_roundtrip_with<M: Model + proptest::arbitrary::Arbitrary>(
    pool: &sqlx::Pool<Sqlite>,
    config: proptest::test_runner::Config,
) -> anyhow::Result<()> {
    use proptest::arbitrary::any;
    use proptest::prop_assert_eq;
    let mut runner = proptest::test_runner::TestRunner::new(config);
    runner.run_async(&any::<M>(), async |model| {
        let mut conn = pool.acquire().await?;
        let row_id = conn.insert(&model).await?;
        prop_assert_eq!(
            &conn.lookup_hash_ids::<M>(&[row_id]).await?,
            &[*model.hash_id()],
        );
        prop_assert_eq!(
            &conn.lookup_row_ids::<M>(&[*model.hash_id()]).await?,
            &[row_id],
        );
        prop_assert_eq!(&conn.fetch_by_row_id::<M>(row_id).await?, &model);
        prop_assert_eq!(
            &conn.fetch_by_hash_id::<M>(model.hash_id()).await?,
            &(row_id, model),
        );
        Ok(())
    })?;
    Ok(())
}

#[cfg(test)]
pub(crate) async fn test_model_roundtrip<M: Model + proptest::arbitrary::Arbitrary>(
    pool: &sqlx::Pool<Sqlite>,
) -> anyhow::Result<()> {
    test_model_roundtrip_with::<M>(pool, Default::default()).await
}
