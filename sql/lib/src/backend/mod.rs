mod convert;
pub mod model;
mod stats;
mod vtab;

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
use balsaq::ConnectionExt as _;
use blake2::Blake2b512;
use blake2::Digest as _;
use clru::CLruCache;
use futures::AsyncRead;
use futures::AsyncReadExt as _;
use futures::StreamExt as _;
use futures::io::Cursor;
use futures::lock::Mutex;
use futures::stream;
use futures::stream::BoxStream;
use itertools::Itertools as _;
use jj_lib::backend::Backend;
use jj_lib::backend::BackendError;
use jj_lib::backend::BackendResult;
use jj_lib::backend::ChangeId;
use jj_lib::backend::Commit;
use jj_lib::backend::CommitId;
use jj_lib::backend::CopyHistory;
use jj_lib::backend::CopyId;
use jj_lib::backend::CopyRecord;
use jj_lib::backend::FileId;
use jj_lib::backend::RelatedCopy;
use jj_lib::backend::SigningFn;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Tree;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::backend::make_root_commit;
use jj_lib::content_hash::blake2b_hash;
use jj_lib::index::Index;
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::repo_path::RepoPathComponentBuf;
use jj_lib::settings::UserSettings;
use pollster::FutureExt as _;
use rusqlite::Connection;
use zerocopy::IntoBytes as _;

pub use self::stats::DbTableStats;
pub use self::stats::Stats;
use crate::convert::JjExt;
use crate::convert::ModelExt as _;
use crate::error::SqlBackendError;

// Blake2b-512 hash of an empty tree — identical to SimpleBackend's constant
// since both use the same content-hashing algorithm.
const EMPTY_TREE_ID_HEX: &str = concat!(
    "482ae5a29fbe856c7272f2071b8b0f0359ee2d89ff392b8a900643fbd0836ecc",
    "d067b8bf41909e206c90d45d6e7d8b6686b93ecaee5fe1a9060d87b672101310",
);

const COMMIT_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(10_000).unwrap();
const TREE_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(50_000).unwrap();

#[derive(Debug)]
pub struct SqlBackend {
    /// Path to the store directory; kept for diagnostics and GC.
    #[allow(dead_code)]
    path: PathBuf,
    /// Database connection. Send + Sync + Clone, wraps Arc internally.
    db: Mutex<Connection>,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
    commits_cache: StdMutex<CLruCache<CommitId, Commit>>,
    trees_cache: StdMutex<CLruCache<TreeId, Tree>>,
}

impl SqlBackend {
    pub fn name() -> &'static str {
        "sql"
    }

    pub fn store_path(&self) -> &Path {
        &self.path
    }

    pub fn connect_db(store_path: &Path) -> Result<Connection, SqlBackendError> {
        let conn = Connection::open(store_path.join("backend.db3"))?;
        conn.busy_timeout(Duration::from_millis(5000))?;
        conn.pragma_update(None, "encoding", "UTF-8")?;
        // page_size must precede journal_mode; ignored on existing databases.
        conn.pragma_update(None, "page_size", 16 * 1024)?; // 16 KiB
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "cache_size", -64 * 1024)?; // 64 MiB (!)
        conn.pragma_update(None, "mmap_size", 1024 * 1024 * 1024)?; // 1 GiB
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        vtab::load_module(&conn)?;
        Ok(conn)
    }

    fn from_parts(store_path: &Path, conn: Connection) -> Self {
        Self {
            path: store_path.to_path_buf(),
            db: Mutex::new(conn),
            root_commit_id: CommitId::from_bytes(&[0; model::COMMIT_ID_LENGTH]),
            root_change_id: ChangeId::from_bytes(&[0; model::CHANGE_ID_LENGTH]),
            empty_tree_id: TreeId::from_hex(EMPTY_TREE_ID_HEX),
            commits_cache: StdMutex::new(CLruCache::new(COMMIT_CACHE_CAPACITY)),
            trees_cache: StdMutex::new(CLruCache::new(TREE_CACHE_CAPACITY)),
        }
    }

    /// Create a new store at `store_path`, initialising the schema.
    pub fn init(_settings: &UserSettings, store_path: &Path) -> Result<Self, SqlBackendError> {
        let conn = Self::connect_db(store_path)?;
        conn.execute_batch(model::SCHEMA)?;
        // Seed the well-known empty tree so that read_tree() can find it
        // without any entries in the nodes table. This mirrors the implicit
        // empty-tree object that Git always has available.
        conn.insert(model::Tree {
            id: TreeId::from_hex(EMPTY_TREE_ID_HEX).to_model()?,
            entries: model::encode_tree_entries(&model::TreeEntries::new())?,
        })?;
        Ok(Self::from_parts(store_path, conn))
    }

    /// Open an existing store at `store_path`.
    pub fn load(_settings: &UserSettings, store_path: &Path) -> Result<Self, SqlBackendError> {
        let conn = Self::connect_db(store_path)?;
        Ok(Self::from_parts(store_path, conn))
    }

    async fn read_object<T, Id>(
        &self,
        id: &Id,
        f: impl AsyncFnOnce(&mut Connection, &Id::Model) -> Result<T, SqlBackendError>,
    ) -> BackendResult<T>
    where
        Id: JjExt + ObjectId,
    {
        let mut conn = self.db.lock().await;
        let model_id = id.to_model().map_err(|e| e.with_read_context(id))?;
        let res = f(&mut conn, &model_id)
            .await
            .map_err(|e| e.with_read_context(id))?;
        Ok(res)
    }

    async fn write_object<T>(
        &self,
        object_type: &'static str,
        f: impl AsyncFnOnce(&mut Connection) -> Result<T, SqlBackendError>,
    ) -> BackendResult<T> {
        let mut conn = self.db.lock().await;
        let res = f(&mut conn)
            .await
            .map_err(|e| e.with_write_context(object_type))?;
        Ok(res)
    }
}

#[async_trait]
impl Backend for SqlBackend {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        model::COMMIT_ID_LENGTH
    }

    fn change_id_length(&self) -> usize {
        model::CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        // Cache hits require no DB access; raise from 1 so jj can read objects
        // in parallel when cache misses occur. The connection pool (TODO) will
        // raise this further once the Mutex<Connection> is replaced.
        1
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        // TODO: Use incremental blob I/O.
        let row = self
            .read_object(id, async |conn, id| {
                let (_row_id, row) = model::File::get_by_id(conn, id)?;
                Ok(row)
            })
            .await?;
        let decompressed = zstd::decode_all(row.content.as_slice())
            .map_err(|e| SqlBackendError::from(e).with_read_context(id))?;
        Ok(Box::pin(Cursor::new(decompressed)))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let (id, content, uncompressed_size) = async {
            let mut buf = vec![0u8; 16 * 1024];
            let mut hasher = Blake2b512::new();
            let mut compressor = zstd::Encoder::new(Vec::new(), 3)?; // 3 is the current default level.
            let mut uncompressed_size: usize = 0;
            loop {
                let n = contents.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                compressor.write_all(&buf[..n])?;
                uncompressed_size += n;
            }
            let id = model::FileId::from(hasher.finalize());
            let compressed = compressor.finish()?;
            let uncompressed_size = uncompressed_size
                .try_into()
                .map_err(SqlBackendError::len_too_large)?;
            Ok((id, compressed, uncompressed_size))
        }
        .await
        .map_err(|e: SqlBackendError| e.with_write_context("file"))?;
        self.write_object("file", async move |conn| {
            conn.insert(model::File {
                id,
                content,
                uncompressed_size,
                simhash: None,
            })?;
            Ok(id.into_jj())
        })
        .await
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        self.read_object(id, async |conn, id| {
            let (_row_id, row) = model::Symlink::get_by_id(conn, id)?;
            Ok(row.target)
        })
        .await
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        self.write_object("symlink", async move |conn| {
            let id = model::SymlinkId::from(blake2b_hash(target));
            conn.insert(model::Symlink {
                id,
                target: target.to_owned(),
            })?;
            Ok(id.into_jj())
        })
        .await
    }

    async fn read_copy(&self, id: &CopyId) -> BackendResult<CopyHistory> {
        self.read_object(id, async |conn, id| {
            let row = conn.get::<model::Copy>(id)?;
            let current_path = RepoPathBuf::from_internal_string(row.current_path)?;
            let parents = model::CopyParent::get_all_for_copy(conn, id)?
                .into_iter()
                .map(|r| r.parent_id.into_jj())
                .collect();
            Ok(CopyHistory {
                current_path,
                parents,
                salt: row.salt,
            })
        })
        .await
    }

    async fn write_copy(&self, copy: &CopyHistory) -> BackendResult<CopyId> {
        self.write_object("copy", async move |conn| {
            let tx = conn.transaction()?;
            let id = model::CopyId::from(blake2b_hash(copy));
            // Insert parents first so the generation subquery can read them.
            for (pos, parent_id) in copy.parents.iter().enumerate() {
                tx.insert(model::CopyParent {
                    copy_id: id,
                    position: pos.try_into().map_err(SqlBackendError::len_too_large)?,
                    parent_id: parent_id.to_model()?,
                })?;
            }
            #[rustfmt::skip]
            let generation: i64 = tx
                .prepare_cached(
                   "SELECT COALESCE(MAX(c.generation), -1) + 1 \
                    FROM copy_parents cp JOIN copies c ON c.id = cp.parent_id \
                    WHERE cp.copy_id = ?1",
                )?
                .query_row((&id,), |row| row.get(0))?;
            tx.insert(model::Copy {
                id,
                generation,
                current_path: copy.current_path.as_internal_file_string().to_owned(),
                salt: copy.salt.clone(),
            })?;
            tx.commit()?;
            Ok(id.into_jj())
        })
        .await
    }

    async fn get_related_copies(&self, id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        self.read_object(id, async |conn, id| {
            // Verify existence.
            conn.get::<model::Copy>(id)?;
            // Walk up to ancestors then down to their descendants; LEFT JOIN
            // copy_parents in the same query so parents are fetched in one pass.
            let result = {
                #[rustfmt::skip]
                let mut stmt = conn.prepare_cached(
                   "WITH RECURSIVE \
                        ancestors(id) AS ( \
                            SELECT ?1 \
                            UNION \
                            SELECT cp.parent_id FROM copy_parents cp \
                                JOIN ancestors a ON cp.copy_id = a.id \
                        ), \
                        related(id) AS ( \
                            SELECT id FROM ancestors \
                            UNION \
                            SELECT cp.copy_id FROM copy_parents cp \
                                JOIN related r ON cp.parent_id = r.id \
                        ) \
                    SELECT c.id, c.current_path, c.salt, p.parent_id \
                    FROM copies c \
                    LEFT JOIN copy_parents p ON p.copy_id = c.id \
                    WHERE c.id IN (SELECT id FROM related) \
                    ORDER BY c.generation DESC, c.id, p.position ASC",
                )?;
                stmt.query_map((id,), |row| {
                    Ok((
                        row.get::<_, model::CopyId>("id")?,
                        row.get::<_, String>("current_path")?,
                        row.get::<_, Vec<u8>>("salt")?,
                        row.get::<_, Option<model::CopyId>>("parent_id")?,
                    ))
                })?
                .process_results(|iter| {
                    iter.chunk_by(|(copy_id, ..)| *copy_id)
                        .into_iter()
                        .map(|(copy_id, mut group)| {
                            let (_, current_path, salt, first_parent) = group.next().unwrap();
                            let current_path = RepoPathBuf::from_internal_string(current_path)?;
                            let parents = first_parent
                                .into_iter()
                                .chain(group.filter_map(|(.., p)| p))
                                .map(CopyId::from_model)
                                .collect();
                            Ok(RelatedCopy {
                                id: copy_id.into_jj(),
                                history: CopyHistory {
                                    current_path,
                                    parents,
                                    salt,
                                },
                            })
                        })
                        .collect::<Result<Vec<_>, SqlBackendError>>()
                })??
            };
            Ok(result)
        })
        .await
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if let Some(tree) = self.trees_cache.lock().unwrap().get(id).cloned() {
            return Ok(tree);
        }
        let tree = self
            .read_object(id, async |conn, id| {
                let (_row_id, row) = model::Tree::get_by_id(conn, id)?;
                let model_entries = model::decode_tree_entries(&row.entries)?;

                // Collect row_ids by type for batch hash lookups.
                let mut file_row_ids: Vec<i64> = Vec::new();
                let mut symlink_row_ids: Vec<i64> = Vec::new();
                let mut tree_row_ids: Vec<i64> = Vec::new();
                let mut commit_row_ids: Vec<i64> = Vec::new();
                for (_, value) in &model_entries {
                    match value {
                        model::TreeValue::File { row_id, .. } => {
                            file_row_ids.push(row_id.0);
                        }
                        model::TreeValue::Symlink(r) => symlink_row_ids.push(r.0),
                        model::TreeValue::Tree(r) => tree_row_ids.push(r.0),
                        model::TreeValue::Submodule(r) => commit_row_ids.push(r.0),
                    }
                }

                let file_hashes = batch_lookup_hashes(
                    conn,
                    "SELECT row_id, id FROM files WHERE row_id IN unpack_i64s(?1)",
                    &file_row_ids,
                )?;
                let symlink_hashes = batch_lookup_hashes(
                    conn,
                    "SELECT row_id, id FROM symlinks WHERE row_id IN unpack_i64s(?1)",
                    &symlink_row_ids,
                )?;
                let tree_hashes = batch_lookup_hashes(
                    conn,
                    "SELECT row_id, id FROM trees WHERE row_id IN unpack_i64s(?1)",
                    &tree_row_ids,
                )?;
                let commit_hashes = batch_lookup_hashes(
                    conn,
                    "SELECT row_id, id FROM commits WHERE row_id IN unpack_i64s(?1)",
                    &commit_row_ids,
                )?;

                // TODO: Use iterators.
                let mut entries = Vec::with_capacity(model_entries.len());
                for (name, value) in model_entries {
                    let jj_value = match value {
                        model::TreeValue::File {
                            row_id,
                            executable,
                            copy_id,
                        } => {
                            let hash = file_hashes[&row_id.0];
                            TreeValue::File {
                                id: model::FileId(hash).into_jj(),
                                executable,
                                copy_id: copy_id
                                    .map(|c| c.into_jj())
                                    .unwrap_or_else(CopyId::placeholder),
                            }
                        }
                        model::TreeValue::Symlink(row_id) => TreeValue::Symlink(
                            model::SymlinkId(symlink_hashes[&row_id.0]).into_jj(),
                        ),
                        model::TreeValue::Tree(row_id) => {
                            TreeValue::Tree(model::TreeId(tree_hashes[&row_id.0]).into_jj())
                        }
                        model::TreeValue::Submodule(row_id) => TreeValue::GitSubmodule(
                            model::CommitId(commit_hashes[&row_id.0]).into_jj(),
                        ),
                    };
                    entries.push((RepoPathComponentBuf::new(name)?, jj_value));
                }
                Ok(Tree::from_sorted_entries(entries))
            })
            .await?;
        self.trees_cache
            .lock()
            .unwrap()
            .put(id.clone(), tree.clone());
        Ok(tree)
    }

    async fn write_tree(&self, _path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        let id = self
            .write_object("tree", async move |conn| {
                let tx = conn.transaction()?;
                let id = model::TreeId::from(blake2b_hash(contents));

                // Collect content IDs by type for batch row_id lookups.
                let mut file_ids: Vec<model::FileId> = Vec::new();
                let mut symlink_ids: Vec<model::SymlinkId> = Vec::new();
                let mut tree_ids: Vec<model::TreeId> = Vec::new();
                let mut commit_ids: Vec<model::CommitId> = Vec::new();
                for entry in contents.entries() {
                    match entry.value() {
                        TreeValue::File { id, .. } => file_ids.push(id.to_model()?),
                        TreeValue::Symlink(id) => symlink_ids.push(id.to_model()?),
                        TreeValue::Tree(id) => tree_ids.push(id.to_model()?),
                        TreeValue::GitSubmodule(id) => commit_ids.push(id.to_model()?),
                    }
                }

                const FILE_SQL: &str = const_format::formatcp!(
                    "SELECT id, row_id FROM files WHERE id IN unpack_blobs(?1, {STRIDE})",
                    STRIDE = model::COMMIT_ID_LENGTH,
                );
                const SYMLINK_SQL: &str = const_format::formatcp!(
                    "SELECT id, row_id FROM symlinks WHERE id IN unpack_blobs(?1, {STRIDE})",
                    STRIDE = model::COMMIT_ID_LENGTH,
                );
                const TREE_SQL: &str = const_format::formatcp!(
                    "SELECT id, row_id FROM trees WHERE id IN unpack_blobs(?1, {STRIDE})",
                    STRIDE = model::COMMIT_ID_LENGTH,
                );
                const COMMIT_SQL: &str = const_format::formatcp!(
                    "SELECT id, row_id FROM commits WHERE id IN unpack_blobs(?1, {STRIDE})",
                    STRIDE = model::COMMIT_ID_LENGTH,
                );

                let file_row_ids = batch_lookup_row_ids(&tx, FILE_SQL, file_ids.as_bytes())?;
                let symlink_row_ids =
                    batch_lookup_row_ids(&tx, SYMLINK_SQL, symlink_ids.as_bytes())?;
                let tree_row_ids = batch_lookup_row_ids(&tx, TREE_SQL, tree_ids.as_bytes())?;
                let commit_row_ids = batch_lookup_row_ids(&tx, COMMIT_SQL, commit_ids.as_bytes())?;

                let mut model_entries = model::TreeEntries::new();
                for entry in contents.entries() {
                    let value = match entry.value() {
                        TreeValue::File {
                            id: file_id,
                            executable,
                            copy_id,
                        } => {
                            let model_id = file_id.to_model()?;
                            model::TreeValue::File {
                                row_id: model::FileRowId(file_row_ids[&model_id.0]),
                                executable: *executable,
                                copy_id: if copy_id.as_bytes().is_empty() {
                                    None
                                } else {
                                    Some(copy_id.to_model()?)
                                },
                            }
                        }
                        TreeValue::Symlink(id) => {
                            let model_id = id.to_model()?;
                            model::TreeValue::Symlink(model::SymlinkRowId(
                                symlink_row_ids[&model_id.0],
                            ))
                        }
                        TreeValue::Tree(id) => {
                            let model_id = id.to_model()?;
                            model::TreeValue::Tree(model::TreeRowId(tree_row_ids[&model_id.0]))
                        }
                        TreeValue::GitSubmodule(id) => {
                            let model_id = id.to_model()?;
                            model::TreeValue::Submodule(model::CommitRowId(
                                commit_row_ids[&model_id.0],
                            ))
                        }
                    };
                    model_entries.push((entry.name().as_internal_str().to_owned(), value));
                }
                let entries_blob = model::encode_tree_entries(&model_entries)?;
                tx.insert(model::Tree {
                    id,
                    entries: entries_blob,
                })?;
                tx.commit()?;
                Ok(id.into_jj())
            })
            .await?;
        self.trees_cache
            .lock()
            .unwrap()
            .put(id.clone(), contents.clone());
        Ok(id)
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }

        if let Some(commit) = self.commits_cache.lock().unwrap().get(id).cloned() {
            return Ok(commit);
        }

        let commit = self
            .read_object(id, async |conn, id| {
                let (_row_id, row) = model::Commit::get_by_id(conn, id)?;

                let (root_trees, conflict_labels): (Vec<TreeId>, Vec<String>) =
                    model::CommitRootTree::get_all_for_commit(conn, id)?
                        .into_iter()
                        .map(|r| (r.tree_id.into_jj(), r.conflict_label))
                        .unzip();

                let parents = model::CommitParent::get_all_for_commit(conn, id)?
                    .into_iter()
                    .map(|r| r.parent_id.into_jj())
                    .collect();
                let predecessors = model::CommitPredecessor::get_all_for_commit(conn, id)?
                    .into_iter()
                    .map(|r| r.predecessor_id.into_jj())
                    .collect();

                Ok(Commit {
                    parents,
                    predecessors,
                    root_tree: Merge::from_vec(root_trees),
                    conflict_labels: Merge::from_vec(conflict_labels),
                    change_id: row.change_id.into_jj(),
                    description: row.description,
                    author: row.author.into_jj(),
                    committer: row.committer.into_jj(),
                    secure_sig: row.secure_sig.map(|sig| sig.into_jj()),
                })
            })
            .await?;

        self.commits_cache
            .lock()
            .unwrap()
            .put(id.clone(), commit.clone());
        Ok(commit)
    }

    async fn write_commit(
        &self,
        commit: Commit,
        _sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let (id, commit) = self
            .write_object("commit", async move |conn| {
                if commit.parents.is_empty() {
                    return Err(SqlBackendError::InternalError(String::from(
                        "cannot write a commit with no parents",
                    )));
                }

                let tx = conn.transaction()?;
                let id = model::CommitId::from(blake2b_hash(&commit));

                tx.insert(model::Commit {
                    id,
                    change_id: commit.change_id.to_model()?,
                    description: commit.description.clone(),
                    author: commit.author.to_model()?,
                    committer: commit.committer.to_model()?,
                    secure_sig: commit
                        .secure_sig
                        .as_ref()
                        .map(|sig| sig.to_model())
                        .transpose()?,
                })?;

                for (pos, (tree_id, label)) in commit
                    .root_tree
                    .iter()
                    .zip(commit.conflict_labels.iter())
                    .enumerate()
                {
                    tx.insert(model::CommitRootTree {
                        commit_id: id,
                        position: pos.try_into().map_err(SqlBackendError::len_too_large)?,
                        tree_id: tree_id.to_model()?,
                        conflict_label: label.clone(),
                    })?;
                }

                for (pos, parent_id) in commit.parents.iter().enumerate() {
                    tx.insert(model::CommitParent {
                        commit_id: id,
                        position: pos.try_into().map_err(SqlBackendError::len_too_large)?,
                        parent_id: parent_id.to_model()?,
                    })?;
                }

                for (pos, pred_id) in commit.predecessors.iter().enumerate() {
                    tx.insert(model::CommitPredecessor {
                        commit_id: id,
                        position: pos.try_into().map_err(SqlBackendError::len_too_large)?,
                        predecessor_id: pred_id.to_model()?,
                    })?;
                }

                tx.commit()?;
                Ok((id.into_jj(), commit))
            })
            .await?;
        self.commits_cache
            .lock()
            .unwrap()
            .put(id.clone(), commit.clone());
        Ok((id, commit))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(stream::empty().boxed())
    }

    fn gc(&self, index: &dyn Index, keep_newer: SystemTime) -> BackendResult<()> {
        let keep_newer_ms = keep_newer
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let head_ids = index
            .all_heads_for_gc()
            .map_err(|e| BackendError::Other(e.into()))?
            .filter(|id| *id != self.root_commit_id)
            .map(|id| id.to_model())
            .collect::<Result<Vec<model::CommitId>, _>>()?;
        let empty_tree_id = self.empty_tree_id.to_model()?;
        let mut conn = self.db.lock().block_on();
        gc_impl(&mut conn, &head_ids, &empty_tree_id, keep_newer_ms)?;
        // Objects deleted by GC may still be in the in-memory caches; flush them
        // so subsequent reads reflect the post-GC state.
        self.commits_cache.lock().unwrap().clear();
        self.trees_cache.lock().unwrap().clear();
        Ok(())
    }
}

fn batch_lookup_hashes(
    conn: &Connection,
    sql: &'static str,
    row_ids: &[i64],
) -> Result<std::collections::HashMap<i64, crate::hash::Hash<64>>, SqlBackendError> {
    if row_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let blob = row_ids.as_bytes();
    let mut stmt = conn.prepare_cached(sql)?;
    let map = stmt
        .query_map((blob,), |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(map)
}

fn batch_lookup_row_ids(
    conn: &Connection,
    sql: &'static str,
    blob: &[u8],
) -> Result<std::collections::HashMap<crate::hash::Hash<64>, i64>, SqlBackendError> {
    if blob.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let mut stmt = conn.prepare_cached(sql)?;
    let map = stmt
        .query_map((blob,), |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(map)
}

fn gc_impl(
    conn: &mut Connection,
    head_ids: &[model::CommitId],
    empty_tree_id: &model::TreeId,
    keep_newer_ms: i64,
) -> Result<(), SqlBackendError> {
    let tx = conn.transaction()?;

    // Phase 1: compute the live commit set.
    tx.execute_batch(
        "DROP TABLE IF EXISTS _gc_live_commits;
         CREATE TEMP TABLE _gc_live_commits (id BLOB PRIMARY KEY);",
    )?;

    {
        let mut stmt = tx.prepare("INSERT OR IGNORE INTO _gc_live_commits VALUES (?1)")?;
        for id in head_ids {
            stmt.execute((id,))?;
        }
    }

    tx.execute_batch(
        "WITH RECURSIVE live(id) AS (
             SELECT id FROM _gc_live_commits
             UNION
             SELECT cp.parent_id FROM commit_parents cp
                 JOIN live ON cp.commit_id = live.id
         )
         INSERT OR IGNORE INTO _gc_live_commits SELECT id FROM live;",
    )?;

    // Delete unreachable commits that are old enough.
    tx.execute(
        "DELETE FROM commits
         WHERE id NOT IN (SELECT id FROM _gc_live_commits)
           AND __last_written_ms <= ?1",
        (keep_newer_ms,),
    )?;

    // Cascade to commit child tables.
    tx.execute_batch(
        "DELETE FROM commit_parents      WHERE commit_id NOT IN (SELECT id FROM commits);
         DELETE FROM commit_predecessors WHERE commit_id NOT IN (SELECT id FROM commits);
         DELETE FROM commit_root_trees   WHERE commit_id NOT IN (SELECT id FROM commits);",
    )?;

    tx.execute_batch("DROP TABLE _gc_live_commits;")?;

    // Phase 2: compute the live tree set via BFS over entries blobs.
    // Seed: empty tree + trees directly referenced by live commits.
    let mut live_tree_rows: std::collections::HashSet<i64> = std::collections::HashSet::new();
    {
        if let Ok(row_id) = tx
            .prepare_cached("SELECT row_id FROM trees WHERE id = ?1")?
            .query_row((empty_tree_id,), |r| r.get::<_, i64>(0))
        {
            live_tree_rows.insert(row_id);
        }
        let mut stmt = tx.prepare_cached(
            "SELECT t.row_id FROM trees t JOIN commit_root_trees crt ON t.id = crt.tree_id",
        )?;
        stmt.query_map([], |r| r.get::<_, i64>(0))?
            .try_for_each(|r| {
                r.map(|id| {
                    live_tree_rows.insert(id);
                })
            })?;
    }

    // TODO: Use table valued function and get rid of all the json_each calls.
    // BFS: for each live tree, decode its entries blob and enqueue child trees.
    // Also collect live file and symlink row_ids from all live trees.
    let mut live_file_rows: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut live_symlink_rows: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut live_copy_ids: std::collections::HashSet<model::CopyId> =
        std::collections::HashSet::new();
    let mut queue: Vec<i64> = live_tree_rows.iter().copied().collect();
    while !queue.is_empty() {
        let current = std::mem::take(&mut queue);
        for tree_row_id in current {
            let entries_blob: Vec<u8> = tx
                .prepare_cached("SELECT entries FROM trees WHERE row_id = ?1")?
                .query_row((tree_row_id,), |r| r.get(0))?;
            let entries =
                model::decode_tree_entries(&entries_blob).map_err(SqlBackendError::from)?;
            for (_, value) in entries {
                match value {
                    model::TreeValue::Tree(child) => {
                        if live_tree_rows.insert(child.0) {
                            queue.push(child.0);
                        }
                    }
                    model::TreeValue::File {
                        row_id, copy_id, ..
                    } => {
                        live_file_rows.insert(row_id.0);
                        if let Some(cid) = copy_id {
                            live_copy_ids.insert(cid);
                        }
                    }
                    model::TreeValue::Symlink(row_id) => {
                        live_symlink_rows.insert(row_id.0);
                    }
                    model::TreeValue::Submodule(_) => {}
                }
            }
        }
    }

    // Also scan surviving orphan trees (too recent to delete) so their
    // referenced files/symlinks are protected even if unreachable from commits.
    {
        let mut stmt = tx.prepare_cached(
            "SELECT entries FROM trees WHERE row_id NOT IN (SELECT value FROM json_each(?1)) AND \
             __last_written_ms > ?2",
        )?;
        let live_ids_json =
            serde_json::to_string(&live_tree_rows.iter().collect::<Vec<_>>()).unwrap();
        stmt.query_map((&live_ids_json, keep_newer_ms), |r| r.get::<_, Vec<u8>>(0))?
            .try_for_each(|blob| -> Result<(), SqlBackendError> {
                let entries = model::decode_tree_entries(&blob?)?;
                for (_, value) in entries {
                    match value {
                        model::TreeValue::File {
                            row_id, copy_id, ..
                        } => {
                            live_file_rows.insert(row_id.0);
                            if let Some(cid) = copy_id {
                                live_copy_ids.insert(cid);
                            }
                        }
                        model::TreeValue::Symlink(row_id) => {
                            live_symlink_rows.insert(row_id.0);
                        }
                        _ => {}
                    }
                }
                Ok(())
            })?;
    }

    // Delete unreachable trees that are old enough.
    {
        let live_ids_json =
            serde_json::to_string(&live_tree_rows.iter().collect::<Vec<_>>()).unwrap();
        tx.execute(
            "DELETE FROM trees WHERE row_id NOT IN (SELECT value FROM json_each(?1)) AND \
             __last_written_ms <= ?2",
            (&live_ids_json, keep_newer_ms),
        )?;
    }

    // Phase 3: delete unreachable files, symlinks, and copies.
    {
        let live_ids_json =
            serde_json::to_string(&live_file_rows.iter().collect::<Vec<_>>()).unwrap();
        tx.execute(
            "DELETE FROM files WHERE row_id NOT IN (SELECT value FROM json_each(?1)) AND \
             __last_written_ms <= ?2",
            (&live_ids_json, keep_newer_ms),
        )?;
    }
    {
        let live_ids_json =
            serde_json::to_string(&live_symlink_rows.iter().collect::<Vec<_>>()).unwrap();
        tx.execute(
            "DELETE FROM symlinks WHERE row_id NOT IN (SELECT value FROM json_each(?1)) AND \
             __last_written_ms <= ?2",
            (&live_ids_json, keep_newer_ms),
        )?;
    }
    {
        let live_ids_json =
            serde_json::to_string(&live_copy_ids.iter().collect::<Vec<_>>()).unwrap();
        tx.execute(
            "DELETE FROM copies WHERE id NOT IN (SELECT value FROM json_each(?1)) AND \
             __last_written_ms <= ?2",
            (&live_ids_json, keep_newer_ms),
        )?;
    }

    tx.execute_batch("DELETE FROM copy_parents WHERE copy_id NOT IN (SELECT id FROM copies);")?;

    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Hash;

    #[test]
    fn test_empty_tree_id() {
        assert_eq!(
            blake2b_hash(&Vec::<()>::new()).as_slice(),
            TreeId::from_hex(EMPTY_TREE_ID_HEX).as_bytes(),
        );
    }

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(model::SCHEMA).unwrap();
        conn
    }

    fn cid(b: u8) -> model::CommitId {
        model::CommitId(Hash([b; model::COMMIT_ID_LENGTH]))
    }

    fn tid(b: u8) -> model::TreeId {
        model::TreeId(Hash([b; model::COMMIT_ID_LENGTH]))
    }

    fn fid(b: u8) -> model::FileId {
        model::FileId(Hash([b; model::COMMIT_ID_LENGTH]))
    }

    fn sid(b: u8) -> model::SymlinkId {
        model::SymlinkId(Hash([b; model::COMMIT_ID_LENGTH]))
    }

    fn bare_commit(id: model::CommitId) -> model::Commit {
        model::Commit {
            id,
            change_id: model::ChangeId(Hash([0u8; model::CHANGE_ID_LENGTH])),
            description: String::new(),
            author: model::Signature {
                name: String::new(),
                email: String::new(),
                timestamp: 0,
                tz_offset: 0,
            },
            committer: model::Signature {
                name: String::new(),
                email: String::new(),
                timestamp: 0,
                tz_offset: 0,
            },
            secure_sig: None,
        }
    }

    /// Insert a commit with one root tree and zero or more parents.
    fn insert_commit(
        conn: &Connection,
        id: model::CommitId,
        parent_ids: &[model::CommitId],
        tree_id: model::TreeId,
    ) {
        conn.insert(bare_commit(id)).unwrap();
        conn.insert(model::CommitRootTree {
            commit_id: id,
            position: 0,
            tree_id,
            conflict_label: String::new(),
        })
        .unwrap();
        for (pos, &parent_id) in parent_ids.iter().enumerate() {
            conn.insert(model::CommitParent {
                commit_id: id,
                position: pos as i64,
                parent_id,
            })
            .unwrap();
        }
    }

    fn commit_exists(conn: &Connection, id: &model::CommitId) -> bool {
        conn.prepare_cached("SELECT 1 FROM commits WHERE id = ?1")
            .unwrap()
            .query_row((id,), |_| Ok(()))
            .is_ok()
    }

    fn tree_exists(conn: &Connection, id: &model::TreeId) -> bool {
        conn.prepare_cached("SELECT 1 FROM trees WHERE id = ?1")
            .unwrap()
            .query_row((id,), |_| Ok(()))
            .is_ok()
    }

    fn file_exists(conn: &Connection, id: &model::FileId) -> bool {
        conn.prepare_cached("SELECT 1 FROM files WHERE id = ?1")
            .unwrap()
            .query_row((id,), |_| Ok(()))
            .is_ok()
    }

    fn symlink_exists(conn: &Connection, id: &model::SymlinkId) -> bool {
        conn.prepare_cached("SELECT 1 FROM symlinks WHERE id = ?1")
            .unwrap()
            .query_row((id,), |_| Ok(()))
            .is_ok()
    }

    fn count(conn: &Connection, table: &str) -> i64 {
        conn.prepare_cached(&format!("SELECT COUNT(*) FROM {table}"))
            .unwrap()
            .query_row([], |r| r.get(0))
            .unwrap()
    }

    /// keep_newer_ms that makes everything old enough to be deleted if
    /// unreachable.
    const OLD: i64 = i64::MAX;
    /// keep_newer_ms that makes everything too recent to be deleted.
    const RECENT: i64 = 0;
    /// A sentinel tree ID used as the empty-tree placeholder in GC calls.
    fn no_empty_tree() -> model::TreeId {
        tid(0xff)
    }

    #[test]
    fn test_gc_keeps_reachable_commits() {
        let mut conn = setup();
        // Chain: A ← B ← C; head = C.
        insert_commit(&conn, cid(1), &[], tid(0x10));
        insert_commit(&conn, cid(2), &[cid(1)], tid(0x20));
        insert_commit(&conn, cid(3), &[cid(2)], tid(0x30));

        gc_impl(&mut conn, &[cid(3)], &no_empty_tree(), OLD).unwrap();

        assert!(commit_exists(&conn, &cid(1)));
        assert!(commit_exists(&conn, &cid(2)));
        assert!(commit_exists(&conn, &cid(3)));
    }

    #[test]
    fn test_gc_deletes_unreachable_old_commits_and_cascades() {
        let mut conn = setup();
        // A ← B (head); A ← C (unreachable).
        insert_commit(&conn, cid(1), &[], tid(0x10));
        insert_commit(&conn, cid(2), &[cid(1)], tid(0x20));
        insert_commit(&conn, cid(3), &[cid(1)], tid(0x30));

        gc_impl(&mut conn, &[cid(2)], &no_empty_tree(), OLD).unwrap();

        assert!(commit_exists(&conn, &cid(1)));
        assert!(commit_exists(&conn, &cid(2)));
        assert!(!commit_exists(&conn, &cid(3)));
        // commit_parents and commit_root_trees rows for C should be gone.
        assert_eq!(
            count(&conn, "commit_parents"),
            1, // only B→A remains
        );
        assert_eq!(
            count(&conn, "commit_root_trees"),
            2, // only A's and B's tree refs remain
        );
    }

    #[test]
    fn test_gc_keeps_recent_unreachable_commits() {
        let mut conn = setup();
        // A ← B (head); A ← C (unreachable but recent).
        insert_commit(&conn, cid(1), &[], tid(0x10));
        insert_commit(&conn, cid(2), &[cid(1)], tid(0x20));
        insert_commit(&conn, cid(3), &[cid(1)], tid(0x30));

        gc_impl(&mut conn, &[cid(2)], &no_empty_tree(), RECENT).unwrap();

        assert!(commit_exists(&conn, &cid(1)));
        assert!(commit_exists(&conn, &cid(2)));
        assert!(commit_exists(&conn, &cid(3)));
    }

    fn insert_tree(
        conn: &Connection,
        id: model::TreeId,
        entries: model::TreeEntries,
    ) -> model::TreeRowId {
        let blob = model::encode_tree_entries(&entries).unwrap();
        conn.insert(model::Tree { id, entries: blob }).unwrap()
    }

    #[test]
    fn test_gc_always_keeps_empty_tree() {
        let mut conn = setup();
        let empty = tid(0xee);
        insert_tree(&conn, empty, model::TreeEntries::new());

        // No heads, no commits — everything else would be deleted.
        gc_impl(&mut conn, &[], &empty, OLD).unwrap();

        assert!(tree_exists(&conn, &empty));
    }

    #[test]
    fn test_gc_keeps_referenced_trees_files_and_symlinks() {
        let mut conn = setup();
        // Commit C → tree T1; T1 contains sub-tree T2 and file F and symlink S.
        let (c, t1, t2, f, s) = (cid(1), tid(1), tid(2), fid(3), sid(4));
        insert_commit(&conn, c, &[], t1);
        let t2_row = insert_tree(&conn, t2, model::TreeEntries::new());
        let f_row = conn
            .insert(model::File {
                id: f,
                content: vec![1, 2, 3],
                uncompressed_size: 3,
                simhash: None,
            })
            .unwrap();
        let s_row = conn
            .insert(model::Symlink {
                id: s,
                target: "target".to_owned(),
            })
            .unwrap();
        insert_tree(
            &conn,
            t1,
            vec![
                ("subdir".to_owned(), model::TreeValue::Tree(t2_row)),
                (
                    "file.txt".to_owned(),
                    model::TreeValue::File {
                        row_id: f_row,
                        executable: false,
                        copy_id: None,
                    },
                ),
                ("link".to_owned(), model::TreeValue::Symlink(s_row)),
            ],
        );

        gc_impl(&mut conn, &[c], &no_empty_tree(), OLD).unwrap();

        assert!(commit_exists(&conn, &c));
        assert!(tree_exists(&conn, &t1));
        assert!(tree_exists(&conn, &t2));
        assert!(file_exists(&conn, &f));
        assert!(symlink_exists(&conn, &s));
    }

    #[test]
    fn test_gc_deletes_orphaned_trees_files_and_symlinks() {
        let mut conn = setup();
        // Insert a tree, file, and symlink with no commit referencing them.
        let (t, f, s) = (tid(1), fid(2), sid(3));
        insert_tree(&conn, t, model::TreeEntries::new());
        conn.insert(model::File {
            id: f,
            content: vec![42],
            uncompressed_size: 1,
            simhash: None,
        })
        .unwrap();
        conn.insert(model::Symlink {
            id: s,
            target: "x".to_owned(),
        })
        .unwrap();

        gc_impl(&mut conn, &[], &no_empty_tree(), OLD).unwrap();

        assert!(!tree_exists(&conn, &t));
        assert!(!file_exists(&conn, &f));
        assert!(!symlink_exists(&conn, &s));
    }

    #[test]
    fn test_gc_keeps_subtrees_transitively() {
        let mut conn = setup();
        // Commit C → T1 → T2 → T3 (three levels deep); only T3 has a file.
        let (c, t1, t2, t3, f) = (cid(1), tid(1), tid(2), tid(3), fid(4));
        insert_commit(&conn, c, &[], t1);
        let f_row = conn
            .insert(model::File {
                id: f,
                content: vec![],
                uncompressed_size: 0,
                simhash: None,
            })
            .unwrap();
        let t3_row = insert_tree(
            &conn,
            t3,
            vec![(
                "c".to_owned(),
                model::TreeValue::File {
                    row_id: f_row,
                    executable: false,
                    copy_id: None,
                },
            )],
        );
        let t2_row = insert_tree(
            &conn,
            t2,
            vec![("b".to_owned(), model::TreeValue::Tree(t3_row))],
        );
        insert_tree(
            &conn,
            t1,
            vec![("a".to_owned(), model::TreeValue::Tree(t2_row))],
        );

        gc_impl(&mut conn, &[c], &no_empty_tree(), OLD).unwrap();

        assert!(tree_exists(&conn, &t1));
        assert!(tree_exists(&conn, &t2));
        assert!(tree_exists(&conn, &t3));
        assert!(file_exists(&conn, &f));
    }
}
