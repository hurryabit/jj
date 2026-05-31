mod convert;
pub mod model;
mod stats;
pub(crate) mod vtab;

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::io::Write as _;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use blake2::Blake2b512;
use blake2::Digest as _;
use clru::CLruCache;
use futures::AsyncRead;
use futures::AsyncReadExt as _;
use futures::StreamExt as _;
use futures::io::Cursor;
use futures::stream;
use futures::stream::BoxStream;
use jj_lib::backend as jj;
use jj_lib::backend::Backend;
use jj_lib::backend::BackendResult;
use jj_lib::backend::make_root_commit;
use jj_lib::content_hash::blake2b_hash;
use jj_lib::index::Index;
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::repo_path::RepoPathComponentBuf;
use jj_lib::settings::UserSettings;
use jj_sql_macro::sql;
use sqlx::Connection as _;
use sqlx::Row as _;
use sqlx::Sqlite;
use sqlx::SqliteConnection;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqliteJournalMode;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::sqlite::SqliteRow;
use sqlx::sqlite::SqliteSynchronous;
use zerocopy::IntoBytes;

pub use self::stats::DbTableStats;
pub use self::stats::FilesStats;
use crate::SimHasher;
use crate::backend::model::CHANGE_ID_LENGTH;
use crate::backend::model::COMMIT_ID_LENGTH;
use crate::backend::model::ChangeId;
use crate::backend::model::Commit;
use crate::backend::model::CommitId;
use crate::backend::model::CommitRowId;
use crate::backend::model::CompressionMode;
use crate::backend::model::CopyHistory;
use crate::backend::model::CopyHistoryRowId;
use crate::backend::model::CopyId;
use crate::backend::model::File;
use crate::backend::model::FileId;
use crate::backend::model::FileRowId;
use crate::backend::model::SqliteConnectionExt;
use crate::backend::model::Symlink;
use crate::backend::model::SymlinkId;
use crate::backend::model::SymlinkRowId;
use crate::backend::model::Tree;
use crate::backend::model::TreeId;
use crate::backend::model::TreeRowId;
use crate::backend::model::TreeValue;
use crate::convert::JjExt;
use crate::convert::ModelExt as _;
use crate::error::SqlBackendError;
use crate::error::SqlBackendResult;
use crate::postcard::Postcard;

const COMMIT_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(10_000).unwrap();
const TREE_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(50_000).unwrap();

// Blake2b-512 hash of the empty tree.
const EMPTY_TREE_ID_HEX: &str = concat!(
    "482ae5a29fbe856c7272f2071b8b0f0359ee2d89ff392b8a900643fbd0836ecc",
    "d067b8bf41909e206c90d45d6e7d8b6686b93ecaee5fe1a9060d87b672101310",
);
// Blake2b-512 hash of the root commit, which is a mostly empty commit.
const ROOT_COMMIT_ID_HEX: &str = concat!(
    "e0bd11de53ab8e7d828975d6a5e7e0075655f150f14a153667d9dc39fe2e9928",
    "ad19f0ff389bdadf77b07cc123d28f2e06955f78bc9a828f099cfc91ce37b7a6",
);

#[derive(Debug)]
pub struct SqlBackend {
    path: PathBuf,
    pool: sqlx::Pool<Sqlite>,
    root_commit_id: jj::CommitId,
    root_change_id: jj::ChangeId,
    empty_tree_id: jj::TreeId,
    commits_cache: StdMutex<CLruCache<CommitId, jj::Commit>>,
    trees_cache: StdMutex<CLruCache<TreeId, jj::Tree>>,
}

impl SqlBackend {
    pub fn name() -> &'static str {
        "sql"
    }

    pub fn store_path(&self) -> &Path {
        &self.path
    }

    pub async fn conn(&self) -> SqlBackendResult<sqlx::pool::PoolConnection<Sqlite>> {
        Ok(self.pool.acquire().await?)
    }

    fn connect_options() -> SqliteConnectOptions {
        SqliteConnectOptions::new()
            .busy_timeout(Duration::from_millis(5000))
            .pragma("encoding", "'UTF-8'")
            .page_size(16 * 1024) // 16 KiB
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .pragma("cache_size", (-64 * 1024).to_string()) // 64 MiB (!)
            .pragma("mmap_size", (1024 * 1024 * 1024).to_string()) // 1 GiB
            .pragma("temp_store", "MEMORY")
    }

    fn pool_options() -> SqlitePoolOptions {
        SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _meta| Box::pin(vtab::sqlx_load_module(conn)))
    }

    async fn connect(
        store_path: &Path,
        connect_options: SqliteConnectOptions,
        pool_options: SqlitePoolOptions,
    ) -> SqlBackendResult<Self> {
        let pool = pool_options
            .connect_with(connect_options.filename(store_path.join("backend.db3")))
            .await?;
        Ok(Self {
            path: store_path.to_path_buf(),
            pool,
            root_commit_id: jj::CommitId::from_hex(ROOT_COMMIT_ID_HEX),
            root_change_id: ChangeId::ZERO.into_jj(),
            empty_tree_id: jj::TreeId::from_hex(EMPTY_TREE_ID_HEX),
            commits_cache: StdMutex::new(CLruCache::new(COMMIT_CACHE_CAPACITY)),
            trees_cache: StdMutex::new(CLruCache::new(TREE_CACHE_CAPACITY)),
        })
    }

    async fn initialize(&self) -> Result<(), SqlBackendError> {
        {
            // NOTE: We need to return the connection to the pool before the writes below.
            let mut conn = self.pool.acquire().await?;
            sqlx::raw_sql(include_str!("../../sql/backend.sql"))
                .execute(&mut *conn)
                .await?;
        }
        let empty_tree_id = self.write_tree(&jj::Tree::default()).await?;
        self.write_commit(make_root_commit(
            self.root_change_id.clone(),
            empty_tree_id.clone(),
        ))
        .await?;
        Ok(())
    }

    pub async fn init_in_memory() -> SqlBackendResult<Self> {
        let backend = Self::connect(
            Path::new(":memory:"),
            Self::connect_options().in_memory(true),
            Self::pool_options(),
        )
        .await?;
        backend.initialize().await?;
        Ok(backend)
    }

    pub async fn init(
        _settings: &UserSettings,
        store_path: &Path,
    ) -> Result<Self, SqlBackendError> {
        let backend = Self::connect(
            store_path,
            Self::connect_options().create_if_missing(true),
            Self::pool_options(),
        )
        .await?;
        backend.initialize().await?;
        Ok(backend)
    }

    pub async fn load(
        _settings: &UserSettings,
        store_path: &Path,
    ) -> Result<Self, SqlBackendError> {
        let backend = Self::connect(
            store_path,
            Self::connect_options().create_if_missing(false),
            Self::pool_options(),
        )
        .await?;
        Ok(backend)
    }

    pub async fn get_root_commit_row_id(&self) -> SqlBackendResult<CommitRowId> {
        let [root_commit_row_id] = self
            .pool
            .acquire()
            .await?
            .lookup_row_ids::<Commit>(&[self.root_commit_id.to_model()?])
            .await?
            .try_into()
            .expect("preservation of length");
        Ok(root_commit_row_id)
    }

    pub async fn get_empty_tree_row_id(&self) -> SqlBackendResult<TreeRowId> {
        let [empty_tree_row_id] = self
            .pool
            .acquire()
            .await?
            .lookup_row_ids::<Tree>(&[self.empty_tree_id.to_model()?])
            .await?
            .try_into()
            .expect("preservation of length");
        Ok(empty_tree_row_id)
    }

    async fn read_file(
        &self,
        id: &jj::FileId,
    ) -> Result<Pin<Box<dyn AsyncRead + Send>>, SqlBackendError> {
        // TODO: Use incremental blob I/O.
        let id = id.to_model()?;
        let (_, file) = self.conn().await?.fetch_by_hash_id::<File>(&id).await?;
        let content = file.decompress(&self.pool).await?;
        let content: Pin<Box<dyn AsyncRead + Send>> = Box::pin(Cursor::new(content));
        Ok(content)
    }

    async fn write_file(
        &self,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<jj::FileId, SqlBackendError> {
        let mut buf = vec![0u8; 16 * 1024];
        let mut hasher = Blake2b512::new();
        let mut sim_hasher = SimHasher::new();
        let mut compressor = zstd::Encoder::new(Vec::new(), zstd::DEFAULT_COMPRESSION_LEVEL)?;
        let mut size: usize = 0;
        loop {
            let n = contents.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            size += n;
            hasher.update(&buf[..n]);
            sim_hasher.update(&buf[..n]);
            compressor.write_all(&buf[..n])?;
        }
        let id = FileId::from(hasher.finalize());
        let size = size.try_into().map_err(SqlBackendError::len_too_large)?;
        let simhash = sim_hasher.finish();
        let compressed_data = compressor.finish()?;

        let file = File {
            id,
            size,
            simhash: Some(simhash),
            compression_mode: CompressionMode::ZSTD,
            compression_base_id: None,
            compressed_data,
        };
        self.conn().await?.insert(&file).await?;
        Ok(id.into_jj())
    }

    async fn read_symlink(&self, id: &jj::SymlinkId) -> Result<String, SqlBackendError> {
        let id = id.to_model()?;
        let (_, row) = self.conn().await?.fetch_by_hash_id::<Symlink>(&id).await?;
        Ok(row.target)
    }

    async fn write_symlink(&self, target: &str) -> Result<jj::SymlinkId, SqlBackendError> {
        let id = SymlinkId::from(blake2b_hash(target));
        let symlink = Symlink {
            id,
            target: target.to_owned(),
        };
        self.conn().await?.insert(&symlink).await?;
        Ok(id.into_jj())
    }

    async fn read_tree(&self, id: &jj::TreeId) -> Result<jj::Tree, SqlBackendError> {
        let id = id.to_model()?;
        if let Some(tree) = self.trees_cache.lock().unwrap().get(&id).cloned() {
            return Ok(tree);
        }
        let mut conn = self.conn().await?;
        let (_, tree) = conn.fetch_by_hash_id::<Tree>(&id).await?;
        let entries = tree.entries.decode()?;

        // Collect row_ids by type for batch hash lookups.
        let mut file_row_ids = Vec::new();
        let mut symlink_row_ids = Vec::new();
        let mut tree_row_ids = Vec::new();
        let mut commit_row_ids = Vec::new();
        for (_, value) in &entries {
            match value {
                TreeValue::File { row_id, .. } => file_row_ids.push(*row_id),
                TreeValue::Symlink(r) => symlink_row_ids.push(*r),
                TreeValue::Tree(r) => tree_row_ids.push(*r),
                TreeValue::Submodule(r) => commit_row_ids.push(*r),
            }
        }

        let mut file_ids = conn
            .lookup_hash_ids::<File>(&file_row_ids)
            .await?
            .into_iter();
        let mut symlink_ids = conn
            .lookup_hash_ids::<Symlink>(&symlink_row_ids)
            .await?
            .into_iter();
        let mut tree_ids = conn
            .lookup_hash_ids::<Tree>(&tree_row_ids)
            .await?
            .into_iter();
        let mut commit_ids = conn
            .lookup_hash_ids::<Commit>(&commit_row_ids)
            .await?
            .into_iter();

        let entries = entries
            .into_iter()
            .map(|(name, value)| {
                let jj_value = match value {
                    TreeValue::File {
                        row_id: _,
                        executable,
                        copy_id,
                    } => {
                        let id = file_ids.next().expect("batch lookup");
                        jj::TreeValue::File {
                            id: id.into_jj(),
                            executable,
                            copy_id: copy_id.into_jj().unwrap_or_else(jj::CopyId::placeholder),
                        }
                    }
                    TreeValue::Symlink(_row_id) => {
                        let id = symlink_ids.next().expect("batch lookup");
                        jj::TreeValue::Symlink(id.into_jj())
                    }
                    TreeValue::Tree(_row_id) => {
                        let id = tree_ids.next().expect("batch lookup");
                        jj::TreeValue::Tree(id.into_jj())
                    }
                    TreeValue::Submodule(_row_id) => {
                        let id = commit_ids.next().expect("batch lookup");
                        jj::TreeValue::GitSubmodule(id.into_jj())
                    }
                };
                Ok((RepoPathComponentBuf::new(name)?, jj_value))
            })
            .collect::<Result<Vec<_>, SqlBackendError>>()?;

        let tree = jj::Tree::from_sorted_entries(entries);
        self.trees_cache.lock().unwrap().put(id, tree.clone());
        Ok(tree)
    }

    async fn write_tree(&self, contents: &jj::Tree) -> Result<jj::TreeId, SqlBackendError> {
        let id = TreeId::from(blake2b_hash(contents));

        let mut file_ids: Vec<FileId> = Vec::new();
        let mut symlink_ids: Vec<SymlinkId> = Vec::new();
        let mut tree_ids: Vec<TreeId> = Vec::new();
        let mut commit_ids: Vec<CommitId> = Vec::new();
        for entry in contents.entries() {
            match entry.value() {
                jj::TreeValue::File { id, .. } => file_ids.push(id.to_model()?),
                jj::TreeValue::Symlink(id) => symlink_ids.push(id.to_model()?),
                jj::TreeValue::Tree(id) => tree_ids.push(id.to_model()?),
                jj::TreeValue::GitSubmodule(id) => commit_ids.push(id.to_model()?),
            }
        }

        let mut conn = self.conn().await?;
        let mut file_row_ids = conn.lookup_row_ids::<File>(&file_ids).await?.into_iter();
        let mut symlink_row_ids = conn
            .lookup_row_ids::<Symlink>(&symlink_ids)
            .await?
            .into_iter();
        let mut tree_row_ids = conn.lookup_row_ids::<Tree>(&tree_ids).await?.into_iter();
        let mut commit_row_ids = conn
            .lookup_row_ids::<Commit>(&commit_ids)
            .await?
            .into_iter();

        let entries = contents
            .entries()
            .map(|entry| {
                let value = match entry.value() {
                    jj::TreeValue::File {
                        id: _,
                        executable,
                        copy_id,
                    } => {
                        let row_id = file_row_ids.next().expect("batch lookup");
                        TreeValue::File {
                            row_id,
                            executable: *executable,
                            copy_id: if copy_id.as_bytes().is_empty() {
                                None
                            } else {
                                Some(copy_id.to_model()?)
                            },
                        }
                    }
                    jj::TreeValue::Symlink(_id) => {
                        let row_id = symlink_row_ids.next().expect("batch lookup");
                        TreeValue::Symlink(row_id)
                    }
                    jj::TreeValue::Tree(_id) => {
                        let row_id = tree_row_ids.next().expect("batch lookup");
                        TreeValue::Tree(row_id)
                    }
                    jj::TreeValue::GitSubmodule(_id) => {
                        let row_id = commit_row_ids.next().expect("batch lookup");
                        TreeValue::Submodule(row_id)
                    }
                };
                Ok((entry.name().as_internal_str().to_owned(), value))
            })
            .collect::<Result<Vec<_>, SqlBackendError>>()?;

        conn.insert(&Tree {
            id,
            entries: Postcard::encode(&entries)?,
        })
        .await?;

        self.trees_cache.lock().unwrap().put(id, contents.clone());
        Ok(id.into_jj())
    }

    async fn read_commit(&self, id: &jj::CommitId) -> Result<jj::Commit, SqlBackendError> {
        let id = id.to_model()?;
        if let Some(commit) = self.commits_cache.lock().unwrap().get(&id).cloned() {
            return Ok(commit);
        }

        let mut conn = self.conn().await?;
        let (_, commit) = conn.fetch_by_hash_id::<Commit>(&id).await?;
        let parents = conn
            .lookup_hash_ids::<Commit>(&commit.parents.decode()?)
            .await?;
        let predecessors = conn
            .lookup_hash_ids::<Commit>(&commit.predecessors.decode()?)
            .await?;
        let root_trees = conn
            .lookup_hash_ids::<Tree>(&commit.root_trees.decode()?)
            .await?;
        let conflict_labels = commit.conflict_labels.decode()?;

        let commit = jj::Commit {
            parents: parents.into_jj(),
            predecessors: predecessors.into_jj(),
            root_tree: Merge::from_vec(root_trees.into_jj()),
            conflict_labels: Merge::from_vec(conflict_labels),
            change_id: commit.change_id.into_jj(),
            description: commit.description,
            author: commit.author.into_jj(),
            committer: commit.committer.into_jj(),
            secure_sig: commit.secure_sig.map(|sig| sig.into_jj()),
        };

        self.commits_cache.lock().unwrap().put(id, commit.clone());
        Ok(commit)
    }

    async fn write_commit(
        &self,
        commit: jj::Commit,
    ) -> Result<(jj::CommitId, jj::Commit), SqlBackendError> {
        let id = CommitId::from(blake2b_hash(&commit));

        let mut conn = self.conn().await?;
        let parents = conn
            .lookup_row_ids::<Commit>(&commit.parents.to_model()?)
            .await?;
        let predecessors = conn
            .lookup_row_ids::<Commit>(&commit.predecessors.to_model()?)
            .await?;
        let root_trees = commit
            .root_tree
            .iter()
            .map(|id| id.to_model())
            .collect::<Result<Vec<_>, _>>()?;
        let root_trees = conn.lookup_row_ids::<Tree>(&root_trees).await?;
        let conflict_labels: Vec<String> = commit.conflict_labels.iter().cloned().collect();

        conn.insert(&Commit {
            id,
            parents: Postcard::encode(&parents)?,
            predecessors: Postcard::encode(&predecessors)?,
            root_trees: Postcard::encode(&root_trees)?,
            conflict_labels: Postcard::encode(&conflict_labels)?,
            change_id: commit.change_id.to_model()?,
            description: commit.description.clone(),
            author: commit.author.to_model()?,
            committer: commit.committer.to_model()?,
            secure_sig: commit.secure_sig.to_model()?,
        })
        .await?;

        self.commits_cache.lock().unwrap().put(id, commit.clone());
        Ok((id.into_jj(), commit))
    }

    async fn read_copy(&self, id: &jj::CopyId) -> Result<jj::CopyHistory, SqlBackendError> {
        let id = id.to_model()?;
        let mut conn = self.conn().await?;
        let (_, copy) = conn.fetch_by_hash_id::<CopyHistory>(&id).await?;
        let parents = conn
            .lookup_hash_ids::<CopyHistory>(&copy.parents.decode()?)
            .await?;
        Ok(jj::CopyHistory {
            current_path: RepoPathBuf::from_internal_string(copy.current_path)?,
            parents: parents.into_jj(),
            salt: copy.salt,
        })
    }

    async fn write_copy(&self, copy: &jj::CopyHistory) -> Result<jj::CopyId, SqlBackendError> {
        let id = CopyId::from(blake2b_hash(copy));
        let mut conn = self.conn().await?;
        let parents = conn
            .lookup_row_ids::<CopyHistory>(&copy.parents.to_model()?)
            .await?;

        let generation: i64 = if parents.is_empty() {
            0
        } else {
            sqlx::query_scalar!(
                "
                SELECT COALESCE(MAX(generation), -1) + 1
                FROM copies
                WHERE row_id IN (SELECT val FROM unpack_i64s WHERE blob = ?)
                ",
                parents.as_bytes(),
            )
            .fetch_one(&mut *conn)
            .await?
        };

        conn.insert(&CopyHistory {
            id,
            generation,
            current_path: copy.current_path.as_internal_file_string().to_owned(),
            parents: Postcard::encode(&parents)?,
            salt: copy.salt.clone(),
        })
        .await?;
        Ok(id.into_jj())
    }

    async fn get_related_copies(&self, id: &jj::CopyId) -> SqlBackendResult<Vec<jj::RelatedCopy>> {
        // TODO: Review this code and add tests for it!
        let id = id.to_model()?;
        let mut conn = self.conn().await?;
        conn.fetch_by_hash_id::<CopyHistory>(&id).await?; // Check a row with the ID exists.
        let rows: Vec<CopyHistory> =
            // TODO: Use macro that checks query.
            sqlx::query_as(
                "
                WITH RECURSIVE
                    ancestors(row_id) AS (
                        SELECT row_id FROM copies WHERE id = ?1
                        UNION
                        SELECT val
                        FROM ancestors, unpack_postcard_i64s((SELECT parents FROM copies WHERE row_id = ancestors.row_id))
                    ),
                    related(row_id) AS (
                        SELECT row_id FROM ancestors
                        UNION
                        SELECT c.row_id
                        FROM copies c, unpack_postcard_i64s(c.parents) p
                        JOIN related r ON p.val = r.row_id
                    )
                SELECT c.id, c.generation, c.current_path, c.parents, c.salt
                FROM copies c
                WHERE c.row_id IN (SELECT row_id FROM related)
                ORDER BY c.generation DESC, c.id
                ",
            )
            .bind(id)
            .fetch_all(&mut *conn)
            .await?;
        let decoded_parents: Vec<Vec<CopyHistoryRowId>> = rows
            .iter()
            .map(|copy| copy.parents.decode())
            .collect::<Result<_, _>>()?;
        let mut seen = HashSet::new();
        let all_parent_row_ids: Vec<CopyHistoryRowId> = decoded_parents
            .iter()
            .flatten()
            .copied()
            .filter(|id| seen.insert(*id))
            .collect();
        let parent_content_ids = conn
            .lookup_hash_ids::<CopyHistory>(&all_parent_row_ids)
            .await?;
        let parent_id_map: HashMap<CopyHistoryRowId, CopyId> = all_parent_row_ids
            .into_iter()
            .zip(parent_content_ids)
            .collect();
        rows.into_iter()
            .zip(decoded_parents)
            .map(|(copy, parent_row_ids)| {
                let current_path = RepoPathBuf::from_internal_string(copy.current_path)?;
                let parents = parent_row_ids
                    .into_iter()
                    .map(|row_id| parent_id_map[&row_id].into_jj())
                    .collect();
                Ok(jj::RelatedCopy {
                    id: copy.id.into_jj(),
                    history: jj::CopyHistory {
                        current_path,
                        parents,
                        salt: copy.salt,
                    },
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }

    async fn gc(&self, index: &dyn Index, keep_newer: SystemTime) -> SqlBackendResult<()> {
        let keep_newer_ms = keep_newer
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let root_commit_row_id = self.get_root_commit_row_id().await?;
        let head_ids = index
            .all_heads_for_gc()?
            .map(|id| id.to_model())
            .collect::<Result<Vec<_>, _>>()?;
        let mut conn = self.conn().await?;
        let head_row_ids = conn.lookup_row_ids::<Commit>(&head_ids).await?;
        gc_impl(&mut conn, root_commit_row_id, &head_row_ids, keep_newer_ms).await?;
        // Objects deleted by GC may still be in the in-memory caches; flush them
        // so subsequent reads reflect the post-GC state.
        self.commits_cache.lock().unwrap().clear();
        self.trees_cache.lock().unwrap().clear();
        Ok(())
    }
}

#[async_trait]
impl Backend for SqlBackend {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        COMMIT_ID_LENGTH
    }

    fn change_id_length(&self) -> usize {
        CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &jj::CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &jj::ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &jj::TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        1
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &jj::FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        self.read_file(id)
            .await
            .map_err(|e| e.with_read_context(id).into())
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<jj::FileId> {
        self.write_file(contents)
            .await
            .map_err(|e| e.with_write_context("file").into())
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &jj::SymlinkId) -> BackendResult<String> {
        self.read_symlink(id)
            .await
            .map_err(|e| e.with_read_context(id).into())
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<jj::SymlinkId> {
        self.write_symlink(target)
            .await
            .map_err(|e| e.with_write_context("symlink").into())
    }

    async fn read_copy(&self, id: &jj::CopyId) -> BackendResult<jj::CopyHistory> {
        self.read_copy(id)
            .await
            .map_err(|e| e.with_read_context(id).into())
    }

    async fn write_copy(&self, copy: &jj::CopyHistory) -> BackendResult<jj::CopyId> {
        self.write_copy(copy)
            .await
            .map_err(|e| e.with_write_context("copy").into())
    }

    async fn get_related_copies(&self, id: &jj::CopyId) -> BackendResult<Vec<jj::RelatedCopy>> {
        self.get_related_copies(id)
            .await
            .map_err(|e: SqlBackendError| e.with_read_context(id).into())
    }

    async fn read_tree(&self, _path: &RepoPath, id: &jj::TreeId) -> BackendResult<jj::Tree> {
        self.read_tree(id)
            .await
            .map_err(|e| e.with_read_context(id).into())
    }

    async fn write_tree(&self, _path: &RepoPath, contents: &jj::Tree) -> BackendResult<jj::TreeId> {
        self.write_tree(contents)
            .await
            .map_err(|e| e.with_write_context("tree").into())
    }

    async fn read_commit(&self, id: &jj::CommitId) -> BackendResult<jj::Commit> {
        self.read_commit(id)
            .await
            .map_err(|e| e.with_read_context(id).into())
    }

    async fn write_commit(
        &self,
        commit: jj::Commit,
        _sign_with: Option<&mut jj::SigningFn>,
    ) -> BackendResult<(jj::CommitId, jj::Commit)> {
        if commit.parents.is_empty() {
            return Err(SqlBackendError::InternalError(String::from(
                "cannot write a commit with no parents",
            ))
            .into());
        }
        self.write_commit(commit)
            .await
            .map_err(|e: SqlBackendError| e.with_write_context("commit").into())
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &jj::CommitId,
        _head: &jj::CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<jj::CopyRecord>>> {
        Ok(stream::empty().boxed())
    }

    fn gc(&self, index: &dyn Index, keep_newer: SystemTime) -> BackendResult<()> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.gc(index, keep_newer))
        })?;
        Ok(())
    }
}

async fn gc_impl(
    conn: &mut SqliteConnection,
    root_commit_row_id: CommitRowId,
    head_row_ids: &[CommitRowId],
    keep_newer_ms: i64,
) -> Result<(), SqlBackendError> {
    let mut tx = conn.begin_with("BEGIN IMMEDIATE").await?;

    // Phase 1: Compute the live commit set. Seed with root commit and heads.
    // TODO: Mark (recent) predessors of recent commits as live too.
    sqlx::query!(
        "
        WITH RECURSIVE live (row_id) AS (
            SELECT ?1
            UNION
            SELECT val FROM unpack_i64s WHERE blob = ?2
            UNION
            SELECT row_id FROM commits WHERE __last_written_ms >= ?3
            UNION
            SELECT u.val
            FROM live
            JOIN commits ON live.row_id = commits.row_id
            JOIN unpack_postcard_i64s AS u ON commits.parents = u.blob
        )
        DELETE FROM commits
        WHERE NOT EXISTS (SELECT 1 FROM live WHERE live.row_id = commits.row_id)
        ",
        root_commit_row_id,
        head_row_ids.as_bytes(),
        keep_newer_ms,
    )
    .execute(&mut *tx)
    .await?;

    // Phase 2: Compute the live tree set via DFS. Seeded with root trees of live commits and trees
    // too recent to delete.
    // TODO: Write a TVF so we can do the DFS using recursive SQL queries.
    let mut live_trees: HashSet<TreeRowId> = sqlx::query(sql!(
        "
        SELECT u.val
        FROM commits
        JOIN unpack_postcard_i64s AS u ON commits.root_trees = u.blob
        UNION
        SELECT row_id
        FROM trees
        WHERE __last_written_ms >= ?1
        "
    ))
    .bind(keep_newer_ms)
    .try_map(|row: SqliteRow| row.try_get(0))
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .collect();
    let mut live_files: HashSet<FileRowId> = HashSet::new();
    let mut live_symlinks: HashSet<SymlinkRowId> = HashSet::new();
    let mut live_copies: HashSet<CopyId> = HashSet::new();

    let mut queue = live_trees.iter().copied().collect::<Vec<_>>();
    while let Some(tree_row_id) = queue.pop() {
        let tree = tx.fetch_by_row_id::<Tree>(tree_row_id).await?;
        for (_, value) in tree.entries.decode()? {
            match value {
                TreeValue::Tree(child) => {
                    if live_trees.insert(child) {
                        queue.push(child);
                    }
                }
                TreeValue::File {
                    row_id, copy_id, ..
                } => {
                    live_files.insert(row_id);
                    live_copies.extend(copy_id);
                }
                TreeValue::Symlink(row_id) => {
                    live_symlinks.insert(row_id);
                }
                TreeValue::Submodule(_) => {}
            }
        }
    }

    // TODO: We need to compute live copies as well.

    // Phase 3: Delete unreachable objects.
    let live_trees = live_trees.into_iter().collect::<Vec<_>>();
    sqlx::query!(
        "
        DELETE FROM trees
        WHERE NOT EXISTS (
            SELECT 1 FROM unpack_i64s AS u WHERE u.val = trees.row_id AND u.blob = ?1
        )
        ",
        live_trees.as_bytes(),
    )
    .execute(&mut *tx)
    .await?;

    let live_files = live_files.into_iter().collect::<Vec<_>>();
    sqlx::query!(
        "
        DELETE FROM files
        WHERE __last_written_ms < ?1 AND NOT EXISTS (
            SELECT 1 FROM unpack_i64s AS u WHERE u.val = files.row_id and u.blob = ?2
        )
        ",
        keep_newer_ms,
        live_files.as_bytes(),
    )
    .execute(&mut *tx)
    .await?;

    let live_symlinks = live_symlinks.into_iter().collect::<Vec<_>>();
    sqlx::query!(
        "
        DELETE FROM symlinks
        WHERE __last_written_ms < ?1 AND NOT EXISTS (
            SELECT 1 FROM unpack_i64s AS u WHERE u.val = symlinks.row_id AND u.blob = ?2
        )
        ",
        keep_newer_ms,
        live_symlinks.as_bytes(),
    )
    .execute(&mut *tx)
    .await?;

    let live_copies = live_copies.into_iter().collect::<Vec<_>>();
    sqlx::query!(
        "
        DELETE FROM copies
        WHERE __last_written_ms < ?1 AND NOT EXISTS (
            SELECT 1
            FROM unpack_blobs AS u
            WHERE u.val = copies.row_id AND u.data = ?2 AND u.stride = ?3
        )
        ",
        keep_newer_ms,
        live_copies.as_bytes(),
        COMMIT_ID_LENGTH as i64,
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::model::ChangeId;
    use crate::backend::model::Signature;
    use crate::backend::model::TreeEntries;
    use crate::backend::model::TreeRowId;
    use crate::hash::Hash;

    #[tokio::test]
    async fn test_seed_roots() -> Result<(), SqlBackendError> {
        // TODO: Also test for root_change_id, here or elsewhere.
        let backend = SqlBackend::init_in_memory().await?;
        let empty_tree = backend.read_tree(&backend.empty_tree_id).await?;
        assert_eq!(empty_tree.entries().count(), 0);
        let root_commit = backend.read_commit(&backend.root_commit_id).await?;
        assert_eq!(
            root_commit.root_tree,
            Merge::resolved(backend.empty_tree_id.clone())
        );
        assert_eq!(root_commit.change_id, backend.root_change_id);
        Ok(())
    }

    fn make_commit(id: u8, parents: &[CommitRowId], root_tree: TreeRowId) -> Commit {
        let author = Signature {
            name: String::new(),
            email: String::new(),
            timestamp: 0,
            tz_offset: 0,
        };
        Commit {
            id: CommitId(Hash([id; _])),
            parents: Postcard::encode(&parents.to_vec()).unwrap(),
            predecessors: Postcard::encode(&Vec::new()).unwrap(),
            root_trees: Postcard::encode(&vec![root_tree]).unwrap(),
            conflict_labels: Postcard::encode(&vec![String::new()]).unwrap(),
            change_id: ChangeId(Hash([0u8; _])),
            description: String::new(),
            committer: author.clone(),
            author,
            secure_sig: None,
        }
    }

    fn make_tree(id: u8, entries: TreeEntries) -> Tree {
        Tree {
            id: TreeId(Hash([id; _])),
            entries: Postcard::encode(&entries).unwrap(),
        }
    }

    fn make_file(id: u8) -> File {
        File {
            id: FileId(Hash([id; _])),
            size: 1,
            simhash: None,
            compression_mode: CompressionMode::NONE,
            compression_base_id: None,
            compressed_data: Vec::from([id]),
        }
    }

    fn make_symlink(id: u8) -> Symlink {
        Symlink {
            id: SymlinkId(Hash([id; _])),
            target: "target".to_owned(),
        }
    }

    /// keep_newer_ms that makes everything old enough to be deleted if
    /// unreachable.
    const OLD: i64 = i64::MAX;
    /// keep_newer_ms that makes everything too recent to be deleted.
    const RECENT: i64 = 0;

    #[tokio::test]
    async fn test_gc_keeps_reachable_commits() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        let cid0 = backend.get_root_commit_row_id().await?;
        let tid0 = backend.get_empty_tree_row_id().await?;
        let mut conn = backend.conn().await?;

        // Chain: A ← B ← C; head = C.
        let cid1 = conn.insert(&make_commit(1, &[cid0], tid0)).await?;
        let cid2 = conn.insert(&make_commit(2, &[cid1], tid0)).await?;
        let cid3 = conn.insert(&make_commit(3, &[cid2], tid0)).await?;

        gc_impl(&mut conn, cid0, &[cid3], OLD).await.unwrap();

        conn.fetch_by_row_id::<Commit>(cid1).await?;
        conn.fetch_by_row_id::<Commit>(cid2).await?;
        conn.fetch_by_row_id::<Commit>(cid3).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_gc_deletes_unreachable_old_commits_and_cascades() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        let cid0 = backend.get_root_commit_row_id().await?;
        let tid0 = backend.get_empty_tree_row_id().await?;
        let mut conn = backend.conn().await?;

        // A ← B (head); A ← C (unreachable).
        let cid1 = conn.insert(&make_commit(1, &[cid0], tid0)).await?;
        let cid2 = conn.insert(&make_commit(2, &[cid1], tid0)).await?;
        let cid3 = conn.insert(&make_commit(3, &[cid1], tid0)).await?;

        gc_impl(&mut conn, cid0, &[cid2], OLD).await.unwrap();

        conn.fetch_by_row_id::<Commit>(cid1).await?;
        conn.fetch_by_row_id::<Commit>(cid2).await?;
        assert!(conn.fetch_by_row_id::<Commit>(cid3).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_gc_keeps_recent_unreachable_commits() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        let cid0 = backend.get_root_commit_row_id().await?;
        let tid0 = backend.get_empty_tree_row_id().await?;
        let mut conn = backend.conn().await?;

        // A ← B (head); A ← C (unreachable but recent).
        let cid1 = conn.insert(&make_commit(1, &[cid0], tid0)).await?;
        let cid2 = conn.insert(&make_commit(2, &[cid1], tid0)).await?;
        let cid3 = conn.insert(&make_commit(3, &[cid1], tid0)).await?;

        gc_impl(&mut conn, cid0, &[cid2], RECENT).await.unwrap();

        conn.fetch_by_row_id::<Commit>(cid1).await?;
        conn.fetch_by_row_id::<Commit>(cid2).await?;
        conn.fetch_by_row_id::<Commit>(cid3).await?;
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_gc_always_keeps_empty_tree() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        let cid0 = backend.get_root_commit_row_id().await?;
        let tid0 = backend.get_empty_tree_row_id().await?;
        let mut conn = backend.conn().await?;

        // No heads, no commits — everything else would be deleted.
        gc_impl(&mut conn, cid0, &[], OLD).await.unwrap();

        conn.fetch_by_row_id::<Tree>(tid0).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_gc_keeps_referenced_trees_files_and_symlinks() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        let cid0 = backend.get_root_commit_row_id().await?;
        let mut conn = backend.conn().await?;

        // jj::Commit C → tree T1; T1 contains sub-tree T2 and file F and symlink S.
        let tid2 = conn.insert(&make_tree(2, TreeEntries::new())).await?;
        let fid1 = conn.insert(&make_file(1)).await?;
        let sid1 = conn.insert(&make_symlink(1)).await?;
        let tid1 = conn
            .insert(&make_tree(
                1,
                vec![
                    ("subdir".to_owned(), TreeValue::Tree(tid2)),
                    (
                        "file.txt".to_owned(),
                        TreeValue::File {
                            row_id: fid1,
                            executable: false,
                            copy_id: None,
                        },
                    ),
                    ("link".to_owned(), TreeValue::Symlink(sid1)),
                ],
            ))
            .await?;
        let cid1 = conn.insert(&make_commit(1, &[cid0], tid1)).await?;

        gc_impl(&mut conn, cid0, &[cid1], OLD).await.unwrap();

        conn.fetch_by_row_id::<Commit>(cid1).await?;
        conn.fetch_by_row_id::<Tree>(tid1).await?;
        conn.fetch_by_row_id::<Tree>(tid2).await?;
        conn.fetch_by_row_id::<File>(fid1).await?;
        conn.fetch_by_row_id::<Symlink>(sid1).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_gc_deletes_orphaned_trees_files_and_symlinks() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        let cid0 = backend.get_root_commit_row_id().await?;
        let mut conn = backend.conn().await?;

        // Insert a tree, file, and symlink with no commit referencing them.
        let tid1 = conn.insert(&make_tree(1, TreeEntries::new())).await?;
        let fid1 = conn.insert(&make_file(1)).await?;
        let sid1 = conn.insert(&make_symlink(1)).await?;

        gc_impl(&mut conn, cid0, &[], OLD).await.unwrap();

        assert!(conn.fetch_by_row_id::<File>(fid1).await.is_err());
        assert!(conn.fetch_by_row_id::<Tree>(tid1).await.is_err());
        assert!(conn.fetch_by_row_id::<Symlink>(sid1).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_gc_keeps_subtrees_transitively() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        let cid0 = backend.get_root_commit_row_id().await?;
        let mut conn = backend.conn().await?;

        // jj::Commit C → T1 → T2 → T3 (three levels deep); only T3 has a file.
        let fid1 = conn.insert(&make_file(1)).await?;
        let tid1 = conn
            .insert(&make_tree(
                1,
                vec![(
                    "c".to_owned(),
                    TreeValue::File {
                        row_id: fid1,
                        executable: false,
                        copy_id: None,
                    },
                )],
            ))
            .await?;
        let tid2 = conn
            .insert(&make_tree(2, vec![("b".to_owned(), TreeValue::Tree(tid1))]))
            .await?;
        let tid3 = conn
            .insert(&make_tree(3, vec![("a".to_owned(), TreeValue::Tree(tid2))]))
            .await?;
        let cid1 = conn.insert(&make_commit(1, &[cid0], tid3)).await?;

        gc_impl(&mut conn, cid0, &[cid1], OLD).await.unwrap();

        conn.fetch_by_row_id::<File>(fid1).await?;
        conn.fetch_by_row_id::<Tree>(tid1).await?;
        conn.fetch_by_row_id::<Tree>(tid2).await?;
        conn.fetch_by_row_id::<Tree>(tid3).await?;
        Ok(())
    }
}
