use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use jj_lib::backend::Backend as _;
use jj_lib::backend::ChangeId;
use jj_lib::backend::Commit;
use jj_lib::backend::CommitId;
use jj_lib::backend::CopyHistory;
use jj_lib::backend::CopyId;
use jj_lib::backend::FileId;
use jj_lib::backend::MillisSinceEpoch;
use jj_lib::backend::Signature;
use jj_lib::backend::Timestamp;
use jj_lib::backend::Tree;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::config::ConfigLayer;
use jj_lib::config::ConfigSource;
use jj_lib::config::StackedConfig;
use jj_lib::index::Index;
use jj_lib::index::IndexResult;
use jj_lib::merge::Merge;
use jj_lib::object_id::HexPrefix;
use jj_lib::object_id::PrefixResolution;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::repo_path::RepoPathComponentBuf;
use jj_lib::revset::ResolvedExpression;
use jj_lib::revset::Revset;
use jj_lib::revset::RevsetEvaluationError;
use jj_lib::settings::UserSettings;
use jj_lib::store::Store;
use jj_sql_lib::SqlBackend;
use pollster::FutureExt as _;
use tempfile::TempDir;

fn user_settings() -> UserSettings {
    let config_text = r#"
        user.name = "Test User"
        user.email = "test.user@example.com"
        operation.username = "test-username"
        operation.hostname = "host.example.com"
        debug.randomness-seed = 42
    "#;
    let mut config = StackedConfig::with_defaults();
    config.add_layer(ConfigLayer::parse(ConfigSource::User, config_text).unwrap());
    UserSettings::from_config(config).unwrap()
}

fn setup() -> (TempDir, SqlBackend) {
    let dir = tempfile::Builder::new()
        .prefix("jj-sql-test-")
        .tempdir()
        .unwrap();
    let backend = SqlBackend::init(&user_settings(), dir.path()).unwrap();
    (dir, backend)
}

fn repo_path(s: &str) -> RepoPathBuf {
    RepoPathBuf::from_internal_string(s).unwrap()
}

fn write_copy(backend: &SqlBackend, current_path: &str, parents: Vec<CopyId>) -> CopyId {
    backend
        .write_copy(&CopyHistory {
            current_path: repo_path(current_path),
            parents,
            salt: vec![],
        })
        .block_on()
        .unwrap()
}

// Round-trip: fields survive a write/read cycle.
#[test]
fn test_copy_round_trip() {
    let (_dir, backend) = setup();

    let parent1 = write_copy(&backend, "old/a.rs", vec![]);
    let parent2 = write_copy(&backend, "old/b.rs", vec![]);
    let id = write_copy(
        &backend,
        "foo/bar.rs",
        vec![parent1.clone(), parent2.clone()],
    );
    let history = backend.read_copy(&id).block_on().unwrap();

    assert_eq!(history.current_path, repo_path("foo/bar.rs"));
    assert_eq!(history.parents, vec![parent1, parent2]);
}

// get_related_copies must not return nodes outside the ancestor/descendant set.
//
// Graph (edges = child → parent):
//   C → A
//   D → A
//   D → B
//
// Starting from C: ancestors = {A}, descendants of {A} = {C, D}.
// B is a parent of D but not an ancestor of C, so it must be excluded.
#[test]
fn test_get_related_copies_excludes_unrelated() {
    let (_dir, backend) = setup();

    let a = write_copy(&backend, "a", vec![]);
    let b = write_copy(&backend, "b", vec![]);
    let c = write_copy(&backend, "c", vec![a.clone()]);
    let d = write_copy(&backend, "d", vec![a.clone(), b.clone()]);

    let related = backend.get_related_copies(&c).block_on().unwrap();
    let ids: Vec<CopyId> = related.into_iter().map(|rc| rc.id).collect();

    assert!(ids.contains(&a), "a (ancestor of c) must be included");
    assert!(ids.contains(&c), "c (the query node) must be included");
    assert!(ids.contains(&d), "d (sibling of c) must be included");
    assert!(
        !ids.contains(&b),
        "b must be excluded — it is not an ancestor of c nor a descendant of any ancestor of c"
    );
}

// get_related_copies must return children before parents (trait contract).
//
// Graph: leaf → middle → root
#[test]
fn test_get_related_copies_children_before_parents() {
    let (_dir, backend) = setup();

    let root = write_copy(&backend, "root", vec![]);
    let middle = write_copy(&backend, "middle", vec![root.clone()]);
    let leaf = write_copy(&backend, "leaf", vec![middle.clone()]);

    let related = backend.get_related_copies(&leaf).block_on().unwrap();
    let ids: Vec<CopyId> = related.into_iter().map(|rc| rc.id).collect();
    let pos = |id: &CopyId| ids.iter().position(|x| x == id).unwrap();

    assert!(pos(&leaf) < pos(&middle), "leaf must come before middle");
    assert!(pos(&middle) < pos(&root), "middle must come before root");
}

// ── GC helpers
// ────────────────────────────────────────────────────────────────

/// A minimal `Index` that returns a fixed set of head commit IDs for GC and
/// panics on every other method (none of which are needed by `SqlBackend::gc`).
struct MockIndex(Vec<CommitId>);

impl Index for MockIndex {
    fn all_heads_for_gc(&self) -> IndexResult<Box<dyn Iterator<Item = CommitId> + '_>> {
        Ok(Box::new(self.0.iter().cloned()))
    }

    fn shortest_unique_commit_id_prefix_len(&self, _: &CommitId) -> IndexResult<usize> {
        unimplemented!()
    }
    fn resolve_commit_id_prefix(&self, _: &HexPrefix) -> IndexResult<PrefixResolution<CommitId>> {
        unimplemented!()
    }
    fn has_id(&self, _: &CommitId) -> IndexResult<bool> {
        unimplemented!()
    }
    fn is_ancestor(&self, _: &CommitId, _: &CommitId) -> IndexResult<bool> {
        unimplemented!()
    }
    fn common_ancestors(&self, _: &[CommitId], _: &[CommitId]) -> IndexResult<Vec<CommitId>> {
        unimplemented!()
    }
    fn heads(&self, _: &mut dyn Iterator<Item = &CommitId>) -> IndexResult<Vec<CommitId>> {
        unimplemented!()
    }
    fn changed_paths_in_commit(
        &self,
        _: &CommitId,
    ) -> IndexResult<Option<Box<dyn Iterator<Item = RepoPathBuf> + '_>>> {
        unimplemented!()
    }
    fn evaluate_revset(
        &self,
        _: &ResolvedExpression,
        _: &Arc<Store>,
    ) -> Result<Box<dyn Revset + '_>, RevsetEvaluationError> {
        unimplemented!()
    }
}

fn make_signature() -> Signature {
    Signature {
        name: String::new(),
        email: String::new(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(0),
            tz_offset: 0,
        },
    }
}

fn write_empty_tree(backend: &SqlBackend) -> TreeId {
    backend
        .write_tree(RepoPath::root(), &Tree::from_sorted_entries(vec![]))
        .block_on()
        .unwrap()
}

fn write_file(backend: &SqlBackend, content: &[u8]) -> FileId {
    let mut cursor = futures::io::Cursor::new(content.to_vec());
    backend
        .write_file(RepoPath::root(), &mut cursor)
        .block_on()
        .unwrap()
}

fn write_tree_with_file(backend: &SqlBackend, file_name: &str, file_id: FileId) -> TreeId {
    let entry = (
        RepoPathComponentBuf::new(file_name).unwrap(),
        TreeValue::File {
            id: file_id,
            executable: false,
            copy_id: CopyId::placeholder(),
        },
    );
    backend
        .write_tree(RepoPath::root(), &Tree::from_sorted_entries(vec![entry]))
        .block_on()
        .unwrap()
}

/// Writes a commit with a `tag` baked into the description so that commits
/// with different tags always get distinct content-addressed IDs.
fn write_commit(
    backend: &SqlBackend,
    tag: &str,
    parent_ids: Vec<CommitId>,
    tree_id: TreeId,
) -> CommitId {
    let commit = Commit {
        parents: parent_ids,
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id),
        conflict_labels: Merge::resolved(String::new()),
        change_id: ChangeId::from_bytes(&[0u8; 16]),
        description: tag.to_owned(),
        author: make_signature(),
        committer: make_signature(),
        secure_sig: None,
    };
    let (id, _) = backend.write_commit(commit, None).block_on().unwrap();
    id
}

/// GC that treats all objects as old enough to be deleted if unreachable.
fn gc_delete_old(backend: &SqlBackend, head_ids: Vec<CommitId>) {
    backend
        .gc(
            &MockIndex(head_ids),
            SystemTime::now() + Duration::from_secs(1),
        )
        .unwrap();
}

/// GC that protects all recently-written objects from deletion.
fn gc_keep_recent(backend: &SqlBackend, head_ids: Vec<CommitId>) {
    backend
        .gc(&MockIndex(head_ids), SystemTime::UNIX_EPOCH)
        .unwrap();
}

// ── GC tests (through the public API) ────────────────────────────────────────

#[test]
fn test_gc_keeps_reachable_commits() {
    let (_dir, backend) = setup();
    let root = backend.root_commit_id().clone();
    let tree = write_empty_tree(&backend);

    let commit_a = write_commit(&backend, "a", vec![root.clone()], tree.clone());
    let commit_b = write_commit(&backend, "b", vec![commit_a.clone()], tree.clone());
    let commit_c = write_commit(&backend, "c", vec![commit_b.clone()], tree);

    gc_delete_old(&backend, vec![commit_c.clone()]);

    backend.read_commit(&commit_a).block_on().unwrap();
    backend.read_commit(&commit_b).block_on().unwrap();
    backend.read_commit(&commit_c).block_on().unwrap();
}

#[test]
fn test_gc_deletes_unreachable_old_commits() {
    let (_dir, backend) = setup();
    let root = backend.root_commit_id().clone();
    let tree = write_empty_tree(&backend);

    // A ← B (head); A ← C (unreachable).
    let commit_a = write_commit(&backend, "a", vec![root.clone()], tree.clone());
    let commit_b = write_commit(&backend, "b", vec![commit_a.clone()], tree.clone());
    let commit_c = write_commit(&backend, "c", vec![commit_a.clone()], tree);

    gc_delete_old(&backend, vec![commit_b.clone()]);

    backend.read_commit(&commit_a).block_on().unwrap();
    backend.read_commit(&commit_b).block_on().unwrap();
    assert!(backend.read_commit(&commit_c).block_on().is_err());
}

#[test]
fn test_gc_keeps_recent_unreachable_commits() {
    let (_dir, backend) = setup();
    let root = backend.root_commit_id().clone();
    let tree = write_empty_tree(&backend);

    let commit_a = write_commit(&backend, "a", vec![root.clone()], tree.clone());
    let commit_b = write_commit(&backend, "b", vec![commit_a.clone()], tree.clone());
    let commit_c = write_commit(&backend, "c", vec![commit_a.clone()], tree);

    // keep_newer = UNIX_EPOCH means nothing qualifies as "old" → nothing deleted.
    gc_keep_recent(&backend, vec![commit_b.clone()]);

    backend.read_commit(&commit_a).block_on().unwrap();
    backend.read_commit(&commit_b).block_on().unwrap();
    backend.read_commit(&commit_c).block_on().unwrap();
}

#[test]
fn test_gc_always_keeps_empty_tree() {
    let (_dir, backend) = setup();
    let empty_tree_id = backend.empty_tree_id().clone();

    // No heads — GC with nothing reachable.
    gc_delete_old(&backend, vec![]);

    backend
        .read_tree(RepoPath::root(), &empty_tree_id)
        .block_on()
        .unwrap();
}

#[test]
fn test_gc_keeps_tree_and_file_for_reachable_commit() {
    let (_dir, backend) = setup();
    let root = backend.root_commit_id().clone();

    let file_id = write_file(&backend, b"hello");
    let tree_id = write_tree_with_file(&backend, "hello.txt", file_id.clone());
    let commit_id = write_commit(&backend, "a", vec![root], tree_id.clone());

    gc_delete_old(&backend, vec![commit_id.clone()]);

    backend.read_commit(&commit_id).block_on().unwrap();
    backend
        .read_tree(RepoPath::root(), &tree_id)
        .block_on()
        .unwrap();
    backend
        .read_file(RepoPath::root(), &file_id)
        .block_on()
        .unwrap();
}

#[test]
fn test_gc_deletes_orphaned_tree_and_file() {
    let (_dir, backend) = setup();

    // Write a file and tree but no commit referencing them.
    let file_id = write_file(&backend, b"orphan");
    let tree_id = write_tree_with_file(&backend, "orphan.txt", file_id.clone());

    gc_delete_old(&backend, vec![]);

    assert!(
        backend
            .read_tree(RepoPath::root(), &tree_id)
            .block_on()
            .is_err()
    );
    assert!(
        backend
            .read_file(RepoPath::root(), &file_id)
            .block_on()
            .is_err()
    );
}

// Diamond: both branches must appear before their shared ancestor.
//
// Graph: d → b → a
//        d → c → a
#[test]
fn test_get_related_copies_diamond_order() {
    let (_dir, backend) = setup();

    let a = write_copy(&backend, "a", vec![]);
    let b = write_copy(&backend, "b", vec![a.clone()]);
    let c = write_copy(&backend, "c", vec![a.clone()]);
    let d = write_copy(&backend, "d", vec![b.clone(), c.clone()]);

    let related = backend.get_related_copies(&d).block_on().unwrap();
    let ids: Vec<CopyId> = related.into_iter().map(|rc| rc.id).collect();
    let pos = |id: &CopyId| ids.iter().position(|x| x == id).unwrap();

    assert!(pos(&d) < pos(&b), "d must come before b");
    assert!(pos(&d) < pos(&c), "d must come before c");
    assert!(pos(&b) < pos(&a), "b must come before a");
    assert!(pos(&c) < pos(&a), "c must come before a");
}
