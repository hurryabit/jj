use std::collections::HashMap;
use std::path::PathBuf;
use std::str;
use std::sync::Arc;

use futures::io::Cursor;
use gix::ObjectId;
use indicatif::HumanCount;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use jj_lib::backend;
use jj_lib::backend::CommitId;
use jj_lib::backend::FileId;
use jj_lib::backend::MillisSinceEpoch;
use jj_lib::backend::Signature;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Timestamp;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::git_backend;
use jj_lib::merge::Merge;
use jj_lib::op_store::RefTarget;
use jj_lib::ref_name::RefName;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo as _;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathComponentBuf;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::store::Store;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::default_working_copy_factory;
use jj_sql_lib::SqlBackend;
use jj_sql_lib::SqlOpHeadsStore;
use jj_sql_lib::SqlOpStore;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, BoxError>;

#[derive(clap::Args, Clone, Debug)]
pub struct ImportGitArgs {
    /// Path to the source git repository (bare or with working copy).
    pub git_repo: PathBuf,

    /// Destination directory for the new jj workspace (default: current
    /// directory).
    pub dest: Option<PathBuf>,
}

pub async fn run(
    settings: &UserSettings,
    cwd: &std::path::Path,
    args: &ImportGitArgs,
) -> Result<()> {
    let git = gix::open(&args.git_repo)?;
    let dest = args.dest.as_deref().unwrap_or(cwd);
    std::fs::create_dir_all(dest)?;

    // Create a full jj workspace at `dest`. This sets up `.jj/` with the SQL
    // backend store, op store, and working copy state (checked out at root, so
    // no files on disk yet).
    let (_workspace, repo) = Workspace::init_with_factories(
        settings,
        dest,
        &|settings, store_path| Ok(Box::new(SqlBackend::init(settings, store_path)?)),
        Signer::from_settings(settings).map_err(|e| Box::new(e) as BoxError)?,
        &|_settings, store_path, root_data| Ok(Box::new(SqlOpStore::init(store_path, root_data)?)),
        &|_settings, store_path, root_op_id| {
            Ok(Box::new(SqlOpHeadsStore::init(store_path, root_op_id)?))
        },
        ReadonlyRepo::default_index_store_initializer(),
        ReadonlyRepo::default_submodule_store_initializer(),
        &*default_working_copy_factory(),
        WorkspaceName::DEFAULT.to_owned(),
    )
    .await?;

    let store = repo.store().clone();

    // Collect all commit-pointing refs as starting heads.
    // Each entry is (full_ref_name, git_commit_oid).
    let git_refs: Vec<(String, ObjectId)> = collect_refs(&git)?;
    eprintln!("Found {} refs.", git_refs.len());

    let git_heads: Vec<ObjectId> = git_refs.iter().map(|(_, id)| *id).collect();

    // BFS from heads → roots to build the (id → parent_ids) map.
    let walk_bar = ProgressBar::new_spinner()
        .with_message("Walking commit graph…")
        .with_style(ProgressStyle::default_spinner().template("{spinner} {msg} {pos}")?);
    walk_bar.enable_steady_tick(std::time::Duration::from_millis(100));
    let parent_map = collect_parent_map(&git, &git_heads, &walk_bar)?;
    let total = parent_map.len();
    walk_bar.finish_with_message(format!("Found {} commits.", HumanCount(total as u64)));

    // Kahn's algorithm: parents-before-children ordering.
    let ordered = topo_sort_parents_first(parent_map)?;

    // Replay all git objects into the SQL store.
    let replay_bar = ProgressBar::new(total as u64).with_style(
        ProgressStyle::default_bar()
            .template("{msg} [{bar:40}] {pos}/{len} ({eta})")?
            .progress_chars("=> "),
    );
    replay_bar.set_message("Replaying commits");
    let mut replayer = Replayer::new(git, store.clone());
    for git_id in &ordered {
        replayer.copy_commit(*git_id).await?;
        replay_bar.inc(1);
    }
    replay_bar.finish_with_message("Replayed commits  ");

    // Record all imported heads in the repo view and publish the operation.
    // This is what makes `tt log` work after import.
    let view_bar = ProgressBar::new_spinner()
        .with_message("Updating view…")
        .with_style(ProgressStyle::default_spinner().template("{spinner} {msg}")?);
    view_bar.enable_steady_tick(std::time::Duration::from_millis(100));
    let mut tx = repo.start_transaction();
    // Only make commits reachable from named refs (branches and tags) visible in
    // the initial view. Remote-tracking refs (refs/remotes/*) and other unnamed
    // refs are imported into the backend for history but are not added as heads,
    // so their commits won't surface as visible divergent ancestors.
    let mut head_commits = Vec::new();
    for (ref_name, git_id) in &git_refs {
        if (ref_name.starts_with("refs/heads/") || ref_name.starts_with("refs/tags/"))
            && let Some(sql_id) = replayer.commit_map.get(git_id)
        {
            head_commits.push(store.get_commit_async(sql_id).await?);
        }
    }
    tx.repo_mut().add_heads(&head_commits).await?;

    // Write bookmarks and tags for each git ref.
    let mut bookmark_count = 0usize;
    let mut tag_count = 0usize;
    for (ref_name, git_id) in &git_refs {
        let Some(sql_id) = replayer.commit_map.get(git_id) else {
            continue;
        };
        let target = RefTarget::normal(sql_id.clone());
        if let Some(name) = ref_name.strip_prefix("refs/heads/") {
            tx.repo_mut()
                .set_local_bookmark_target(RefName::new(name), target);
            bookmark_count += 1;
        } else if let Some(name) = ref_name.strip_prefix("refs/tags/") {
            tx.repo_mut()
                .set_local_tag_target(RefName::new(name), target);
            tag_count += 1;
        }
        // refs/remotes/* and other refs are skipped — no jj equivalent.
    }
    tx.commit("import git repository").await?;
    view_bar.finish_and_clear();

    eprintln!(
        "Done. {} commits, {} bookmarks, {} tags.",
        HumanCount(total as u64),
        HumanCount(bookmark_count as u64),
        HumanCount(tag_count as u64),
    );
    Ok(())
}

// ── DAG collection
// ────────────────────────────────────────────────────────────

fn collect_refs(git: &gix::Repository) -> Result<Vec<(String, ObjectId)>> {
    let refs = git
        .references()?
        .all()?
        .filter_map(|r| r.ok())
        .filter_map(|r| {
            let name = r.name().as_bstr().to_string();
            r.into_fully_peeled_id().ok().map(|id| (name, id))
        })
        .filter(|(_, id)| {
            id.object()
                .map(|o| o.kind == gix::object::Kind::Commit)
                .unwrap_or(false)
        })
        .map(|(name, id)| (name, id.detach()))
        .collect();
    Ok(refs)
}

/// BFS from `heads` toward roots, collecting each commit's parent IDs.
///
/// This is a single O(N) pass over the object store. We avoid `gix::rev_walk`
/// so we don't need the `revision` feature flag.
fn collect_parent_map(
    git: &gix::Repository,
    heads: &[ObjectId],
    bar: &ProgressBar,
) -> Result<HashMap<ObjectId, Vec<ObjectId>>> {
    let mut parent_map: HashMap<ObjectId, Vec<ObjectId>> = HashMap::new();
    let mut queue = heads.to_vec();

    while let Some(id) = queue.pop() {
        if parent_map.contains_key(&id) {
            continue;
        }
        let obj = git.find_object(id)?;
        let commit = obj.try_to_commit_ref()?;
        let parents: Vec<ObjectId> = commit.parents().map(|oid| oid.to_owned()).collect();
        for parent in &parents {
            if !parent_map.contains_key(parent) {
                queue.push(*parent);
            }
        }
        parent_map.insert(id, parents);
        bar.inc(1);
    }

    Ok(parent_map)
}

// ── topo sort
// ─────────────────────────────────────────────────────────────────

/// Kahn's algorithm. Returns commit IDs with parents always before children.
///
/// Simple `rev_walk(...).reverse()` is wrong for diamond-shaped DAGs; this
/// is O(N + E) and correct for any DAG including shallow clones.
fn topo_sort_parents_first(parent_map: HashMap<ObjectId, Vec<ObjectId>>) -> Result<Vec<ObjectId>> {
    // in_degree[id] counts how many of id's parents are still unprocessed.
    let mut in_degree: HashMap<ObjectId, usize> = parent_map.keys().map(|id| (*id, 0)).collect();
    let mut children_of: HashMap<ObjectId, Vec<ObjectId>> = HashMap::new();

    for (id, parents) in &parent_map {
        for parent in parents {
            if in_degree.contains_key(parent) {
                *in_degree.get_mut(id).unwrap() += 1;
                children_of.entry(*parent).or_default().push(*id);
            }
            // Parents outside the set (shallow-clone boundary) are ignored.
        }
    }

    // Seeds: commits whose parents are all outside the set (roots).
    let mut queue: Vec<ObjectId> = in_degree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(id, _)| *id)
        .collect();

    let mut result = Vec::with_capacity(parent_map.len());
    while let Some(id) = queue.pop() {
        result.push(id);
        for child in children_of.get(&id).into_iter().flatten() {
            let d = in_degree.get_mut(child).unwrap();
            *d -= 1;
            if *d == 0 {
                queue.push(*child);
            }
        }
    }

    if result.len() != parent_map.len() {
        return Err("cycle in commit graph — should not happen in a valid git repository".into());
    }
    Ok(result)
}

// ── Replayer
// ──────────────────────────────────────────────────────────────────

struct Replayer {
    git: gix::Repository,
    store: Arc<Store>,
    /// git blob id → SQL file id (deduplicates shared blobs across all trees)
    file_map: HashMap<ObjectId, FileId>,
    /// git blob id → SQL symlink id
    symlink_map: HashMap<ObjectId, SymlinkId>,
    /// git tree id → SQL tree id (deduplicates shared subtrees across commits)
    tree_map: HashMap<ObjectId, TreeId>,
    /// git commit id → SQL commit id
    pub commit_map: HashMap<ObjectId, CommitId>,
}

impl Replayer {
    fn new(git: gix::Repository, store: Arc<Store>) -> Self {
        Self {
            git,
            store,
            file_map: HashMap::new(),
            symlink_map: HashMap::new(),
            tree_map: HashMap::new(),
            commit_map: HashMap::new(),
        }
    }

    async fn copy_file(&mut self, git_id: ObjectId) -> Result<FileId> {
        if let Some(id) = self.file_map.get(&git_id) {
            return Ok(id.clone());
        }
        let data = self.git.find_object(git_id)?.try_into_blob()?.take_data();
        let id = self
            .store
            .write_file(RepoPath::root(), &mut Cursor::new(data))
            .await?;
        self.file_map.insert(git_id, id.clone());
        Ok(id)
    }

    async fn copy_symlink(&mut self, git_id: ObjectId) -> Result<SymlinkId> {
        if let Some(id) = self.symlink_map.get(&git_id) {
            return Ok(id.clone());
        }
        let data = self.git.find_object(git_id)?.try_into_blob()?.take_data();
        let target = String::from_utf8(data)?;
        let id = self.store.write_symlink(RepoPath::root(), &target).await?;
        self.symlink_map.insert(git_id, id.clone());
        Ok(id)
    }

    /// Post-order DFS over a git tree.
    ///
    /// `tree_map` deduplicates shared subtrees (the common case — most commits
    /// share large subtrees with their parent), so each unique tree object is
    /// written to the SQL store at most once.
    async fn copy_tree(&mut self, git_id: ObjectId) -> Result<TreeId> {
        if let Some(id) = self.tree_map.get(&git_id) {
            return Ok(id.clone());
        }

        // Decode entries eagerly into owned data before any await points.
        // This avoids lifetime conflicts between the gix object and self borrows.
        let raw_entries: Vec<(Vec<u8>, gix::object::tree::EntryMode, ObjectId)> = {
            let obj = self.git.find_object(git_id)?;
            let git_tree = obj.try_into_tree()?;
            git_tree
                .iter()
                .map(|entry| {
                    let entry = entry?;
                    Ok((
                        entry.filename().to_vec(),
                        entry.mode(),
                        entry.oid().to_owned(),
                    ))
                })
                .collect::<std::result::Result<_, gix::objs::decode::Error>>()?
        };

        let mut jj_entries = Vec::with_capacity(raw_entries.len());
        for (filename, mode, oid) in raw_entries {
            let name = RepoPathComponentBuf::new(str::from_utf8(&filename)?)?;
            let value = match mode.kind() {
                gix::object::tree::EntryKind::Blob => TreeValue::File {
                    id: self.copy_file(oid).await?,
                    executable: false,
                    copy_id: backend::CopyId::placeholder(),
                },
                gix::object::tree::EntryKind::BlobExecutable => TreeValue::File {
                    id: self.copy_file(oid).await?,
                    executable: true,
                    copy_id: backend::CopyId::placeholder(),
                },
                gix::object::tree::EntryKind::Link => {
                    TreeValue::Symlink(self.copy_symlink(oid).await?)
                }
                gix::object::tree::EntryKind::Tree => {
                    // Box::pin is required for recursive async functions.
                    TreeValue::Tree(Box::pin(self.copy_tree(oid)).await?)
                }
                gix::object::tree::EntryKind::Commit => {
                    // Git submodule pointer. The git SHA-1 is not a valid commit
                    // ID in the SQL backend (wrong hash length), and the submodule
                    // content is not being imported, so skip the entry entirely.
                    continue;
                }
            };
            jj_entries.push((name, value));
        }

        // git sorts directory entries as if they have a trailing '/';
        // re-sort by jj's rules (plain lexicographic order on component names).
        jj_entries.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        let jj_tree = self
            .store
            .write_tree(
                RepoPath::root(),
                backend::Tree::from_sorted_entries(jj_entries),
            )
            .await?;
        let id = jj_tree.id().clone();
        self.tree_map.insert(git_id, id.clone());
        Ok(id)
    }

    /// Copies one git commit into the SQL store.
    ///
    /// All parents must already be in `self.commit_map` (guaranteed by the
    /// topo-sorted ordering from `topo_sort_parents_first`).
    async fn copy_commit(&mut self, git_id: ObjectId) -> Result<()> {
        if self.commit_map.contains_key(&git_id) {
            return Ok(());
        }

        // Decode everything from the git commit into owned values before
        // issuing any await points (git object borrows are not Send).
        let (git_parents, git_tree, description, author, committer, change_id) = {
            let obj = self.git.find_object(git_id)?;
            let commit = obj.try_to_commit_ref()?;
            let git_parents: Vec<ObjectId> = commit.parents().map(|oid| oid.to_owned()).collect();
            let git_tree = commit.tree().to_owned();
            let description = String::from_utf8_lossy(commit.message).into_owned();
            let author = convert_signature(commit.author()?);
            let committer = convert_signature(commit.committer()?);
            // Use the `change-id` extra header written by jj, falling back to
            // the same synthetic derivation jj uses for plain git commits.
            let change_id =
                git_backend::extract_change_id_from_commit(&commit).unwrap_or_else(|| {
                    git_backend::synthetic_change_id_from_git_commit_id(&CommitId::from_bytes(
                        git_id.as_bytes(),
                    ))
                });
            (
                git_parents,
                git_tree,
                description,
                author,
                committer,
                change_id,
            )
        };

        // Translate git parent IDs → SQL parent IDs.
        // The topo sort guarantees every parent is already in commit_map.
        let parents: Vec<CommitId> = if git_parents.is_empty() {
            vec![self.store.root_commit_id().clone()]
        } else {
            git_parents
                .iter()
                .map(|git_parent| {
                    self.commit_map
                        .get(git_parent)
                        .cloned()
                        .ok_or("parent not in commit_map — topo sort bug")
                })
                .collect::<std::result::Result<_, _>>()
                .map_err(|e: &str| BoxError::from(e))?
        };

        let root_tree_id = self.copy_tree(git_tree).await?;

        let jj_commit = self
            .store
            .write_commit(
                backend::Commit {
                    parents,
                    predecessors: vec![],
                    root_tree: Merge::resolved(root_tree_id),
                    conflict_labels: Merge::resolved(String::new()),
                    change_id,
                    description,
                    author,
                    committer,
                    secure_sig: None,
                },
                None,
            )
            .await?;

        self.commit_map.insert(git_id, jj_commit.id().clone());
        Ok(())
    }
}

// ── helpers
// ───────────────────────────────────────────────────────────────────

fn convert_signature(sig: gix::actor::SignatureRef<'_>) -> Signature {
    let time = sig.time().unwrap_or_default();
    Signature {
        name: String::from_utf8_lossy(sig.name).into_owned(),
        email: String::from_utf8_lossy(sig.email).into_owned(),
        timestamp: Timestamp {
            // git stores UTC epoch seconds; jj stores milliseconds.
            timestamp: MillisSinceEpoch(time.seconds * 1000),
            // git offset is in seconds; jj stores minutes.
            tz_offset: time.offset.div_euclid(60),
        },
    }
}
