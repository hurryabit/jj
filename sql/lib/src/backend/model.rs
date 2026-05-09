use std::convert::TryFrom;

use balsaq::ConnectionExt as _;
use balsaq::Model as _;
use rusqlite::Connection;

use crate::hash::Hash;
use crate::id_newtype;

pub const COMMIT_ID_LENGTH: usize = 64;
pub const CHANGE_ID_LENGTH: usize = 16;

id_newtype!(FileId, COMMIT_ID_LENGTH, true);
id_newtype!(SymlinkId, COMMIT_ID_LENGTH, true);
id_newtype!(TreeId, COMMIT_ID_LENGTH, true);
id_newtype!(CommitId, COMMIT_ID_LENGTH, true);
id_newtype!(CopyId, COMMIT_ID_LENGTH, true);
id_newtype!(ChangeId, CHANGE_ID_LENGTH, false);

#[balsaq::schema]
mod schema {
    use super::*;

    #[balsaq::group]
    pub struct Signature {
        pub name: String,
        pub email: String,
        pub timestamp: i64,
        pub tz_offset: i32,
    }

    #[balsaq::group]
    pub struct SecureSig {
        pub data: Vec<u8>,
        pub sig: Vec<u8>,
    }

    #[balsaq::table("files", track_last_update)]
    pub struct File {
        #[primary_key]
        pub id: FileId,
        pub content: Vec<u8>,
        pub uncompressed_size: i64,
        /// SimHash fingerprint of the uncompressed content, used to find
        /// delta-compression base candidates by Hamming distance.
        pub simhash: i64,
    }

    #[balsaq::table("symlinks", track_last_update)]
    pub struct Symlink {
        #[primary_key]
        pub id: SymlinkId,
        pub target: String,
    }

    #[balsaq::table("trees", track_last_update)]
    pub struct Tree {
        #[primary_key]
        pub id: TreeId,
    }

    impl Tree {
        /// Returns `Ok(())` if the tree exists, or `Err(QueryReturnedNoRows)`
        /// if not.
        pub fn exists(conn: &Connection, id: &TreeId) -> rusqlite::Result<()> {
            conn.prepare_cached("SELECT 1 FROM trees WHERE id = ?1")?
                .query_row((id,), |_| Ok(()))
        }
    }

    pub enum TreeValue {
        File {
            file_id: FileId,
            executable: bool,
            copy_id: Option<CopyId>,
        },
        Symlink(SymlinkId),
        Tree(TreeId),
        Submodule(CommitId),
    }

    #[repr(i64)]
    #[derive(balsaq::Column, Clone, Copy)]
    enum NodeKind {
        File = 0,
        ExecutableFile = 1,
        Symlink = 2,
        Tree = 3,
        Submodule = 4,
    }

    #[balsaq::group]
    struct TreeValueRaw {
        kind: NodeKind,
        id: Hash<COMMIT_ID_LENGTH>,
        copy_id: Option<CopyId>,
    }

    impl TryFrom<TreeValueRaw> for TreeValue {
        type Error = rusqlite::Error;

        fn try_from(raw: TreeValueRaw) -> rusqlite::Result<Self> {
            Ok(match raw.kind {
                NodeKind::File => TreeValue::File {
                    file_id: FileId(raw.id),
                    executable: false,
                    copy_id: raw.copy_id,
                },
                NodeKind::ExecutableFile => TreeValue::File {
                    file_id: FileId(raw.id),
                    executable: true,
                    copy_id: raw.copy_id,
                },
                NodeKind::Symlink => TreeValue::Symlink(SymlinkId(raw.id)),
                NodeKind::Tree => TreeValue::Tree(TreeId(raw.id)),
                NodeKind::Submodule => TreeValue::Submodule(CommitId(raw.id)),
            })
        }
    }

    impl From<TreeValue> for TreeValueRaw {
        fn from(v: TreeValue) -> Self {
            match v {
                TreeValue::File {
                    file_id,
                    executable,
                    copy_id,
                } => TreeValueRaw {
                    kind: if executable {
                        NodeKind::ExecutableFile
                    } else {
                        NodeKind::File
                    },
                    id: file_id.0,
                    copy_id,
                },
                TreeValue::Symlink(id) => TreeValueRaw {
                    kind: NodeKind::Symlink,
                    id: id.0,
                    copy_id: None,
                },
                TreeValue::Tree(id) => TreeValueRaw {
                    kind: NodeKind::Tree,
                    id: id.0,
                    copy_id: None,
                },
                TreeValue::Submodule(id) => TreeValueRaw {
                    kind: NodeKind::Submodule,
                    id: id.0,
                    copy_id: None,
                },
            }
        }
    }

    #[balsaq::table("tree_entries")]
    pub struct TreeEntry {
        #[primary_key]
        pub tree_id: TreeId,
        #[primary_key]
        pub name: String,
        #[group(via = TreeValueRaw)]
        pub value: TreeValue,
    }

    impl TreeEntry {
        pub fn get_all_for_tree(
            conn: &Connection,
            tree_id: &TreeId,
        ) -> rusqlite::Result<Vec<Self>> {
            const SQL: &str =
                const_format::concatcp!(TreeEntry::SELECT, " WHERE tree_id = ?1 ORDER BY name");
            conn.get_all(SQL, (tree_id,))
        }
    }

    #[balsaq::table("commits", track_last_update)]
    #[index(change_id)]
    pub struct Commit {
        #[primary_key]
        pub id: CommitId,
        pub change_id: ChangeId,
        pub description: String,
        #[group]
        pub author: Signature,
        #[group]
        pub committer: Signature,
        #[group]
        pub secure_sig: Option<SecureSig>,
    }

    #[balsaq::table("commit_root_trees")]
    pub struct CommitRootTree {
        #[primary_key]
        pub commit_id: CommitId,
        #[primary_key]
        pub position: i64,
        pub tree_id: TreeId,
        pub conflict_label: String,
    }

    impl CommitRootTree {
        pub fn get_all_for_commit(
            conn: &Connection,
            commit_id: &CommitId,
        ) -> rusqlite::Result<Vec<Self>> {
            const SQL: &str = const_format::concatcp!(
                CommitRootTree::SELECT,
                " WHERE commit_id = ?1 ORDER BY position ASC"
            );
            conn.get_all(SQL, (commit_id,))
        }
    }

    #[balsaq::table("commit_parents")]
    #[index(parent_id)]
    pub struct CommitParent {
        #[primary_key]
        pub commit_id: CommitId,
        #[primary_key]
        pub position: i64,
        pub parent_id: CommitId,
    }

    impl CommitParent {
        pub fn get_all_for_commit(
            conn: &Connection,
            commit_id: &CommitId,
        ) -> rusqlite::Result<Vec<Self>> {
            const SQL: &str = const_format::concatcp!(
                CommitParent::SELECT,
                " WHERE commit_id = ?1 ORDER BY position ASC"
            );
            conn.get_all(SQL, (commit_id,))
        }
    }

    #[balsaq::table("commit_predecessors")]
    pub struct CommitPredecessor {
        #[primary_key]
        pub commit_id: CommitId,
        #[primary_key]
        pub position: i64,
        pub predecessor_id: CommitId,
    }

    impl CommitPredecessor {
        pub fn get_all_for_commit(
            conn: &Connection,
            commit_id: &CommitId,
        ) -> rusqlite::Result<Vec<Self>> {
            const SQL: &str = const_format::concatcp!(
                CommitPredecessor::SELECT,
                " WHERE commit_id = ?1 ORDER BY position ASC"
            );
            conn.get_all(SQL, (commit_id,))
        }
    }

    #[balsaq::table("copies", track_last_update)]
    pub struct Copy {
        #[primary_key]
        pub id: CopyId,
        pub generation: i64,
        pub current_path: String,
        pub salt: Vec<u8>,
    }

    #[balsaq::table("copy_parents")]
    #[index(parent_id)]
    pub struct CopyParent {
        #[primary_key]
        pub copy_id: CopyId,
        #[primary_key]
        pub position: i64,
        pub parent_id: CopyId,
    }

    impl CopyParent {
        pub fn get_all_for_copy(
            conn: &Connection,
            copy_id: &CopyId,
        ) -> rusqlite::Result<Vec<Self>> {
            const SQL: &str = const_format::concatcp!(
                CopyParent::SELECT,
                " WHERE copy_id = ?1 ORDER BY position ASC"
            );
            conn.get_all(SQL, (copy_id,))
        }
    }
}

pub use schema::*;

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;

    use super::*;
    use crate::hash::Hash;

    #[test]
    fn schema() {
        assert_snapshot!(SCHEMA);
    }

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(super::SCHEMA).unwrap();
        conn
    }

    fn fid(b: u8) -> FileId {
        FileId(Hash([b; COMMIT_ID_LENGTH]))
    }
    fn sid(b: u8) -> SymlinkId {
        SymlinkId(Hash([b; COMMIT_ID_LENGTH]))
    }
    fn tid(b: u8) -> TreeId {
        TreeId(Hash([b; COMMIT_ID_LENGTH]))
    }
    fn cid(b: u8) -> CommitId {
        CommitId(Hash([b; COMMIT_ID_LENGTH]))
    }
    fn cpid(b: u8) -> CopyId {
        CopyId(Hash([b; COMMIT_ID_LENGTH]))
    }
    fn chid(b: u8) -> ChangeId {
        ChangeId(Hash([b; CHANGE_ID_LENGTH]))
    }

    #[test]
    fn tree_roundtrip() {
        let conn = setup();
        conn.insert(Tree { id: tid(3) }).unwrap();
        Tree::exists(&conn, &tid(3)).unwrap();
        assert!(Tree::exists(&conn, &tid(99)).is_err());
    }

    fn make_node(tree: TreeId, name: &str, value: TreeValue) -> TreeEntry {
        TreeEntry {
            tree_id: tree,
            name: name.to_owned(),
            value,
        }
    }

    #[test]
    fn node_file_non_executable_no_copy_id() {
        let conn = setup();
        let tree = tid(1);
        conn.insert(make_node(
            tree,
            "a.txt",
            TreeValue::File {
                file_id: fid(2),
                executable: false,
                copy_id: None,
            },
        ))
        .unwrap();
        let rows = TreeEntry::get_all_for_tree(&conn, &tree).unwrap();
        assert_eq!(rows.len(), 1);
        match rows[0].value {
            TreeValue::File {
                file_id,
                executable,
                copy_id,
            } => {
                assert_eq!(file_id, fid(2));
                assert!(!executable);
                assert!(copy_id.is_none());
            }
            _ => panic!("expected File"),
        }
    }

    #[test]
    fn node_file_non_executable_with_copy_id() {
        let conn = setup();
        let tree = tid(1);
        conn.insert(make_node(
            tree,
            "a.txt",
            TreeValue::File {
                file_id: fid(2),
                executable: false,
                copy_id: Some(cpid(5)),
            },
        ))
        .unwrap();
        let rows = TreeEntry::get_all_for_tree(&conn, &tree).unwrap();
        match rows[0].value {
            TreeValue::File {
                file_id,
                executable,
                copy_id,
            } => {
                assert_eq!(file_id, fid(2));
                assert!(!executable);
                assert_eq!(copy_id, Some(cpid(5)));
            }
            _ => panic!("expected File"),
        }
    }

    #[test]
    fn node_file_executable_no_copy_id() {
        let conn = setup();
        let tree = tid(1);
        conn.insert(make_node(
            tree,
            "run.sh",
            TreeValue::File {
                file_id: fid(3),
                executable: true,
                copy_id: None,
            },
        ))
        .unwrap();
        let rows = TreeEntry::get_all_for_tree(&conn, &tree).unwrap();
        match rows[0].value {
            TreeValue::File {
                file_id,
                executable,
                copy_id,
            } => {
                assert_eq!(file_id, fid(3));
                assert!(executable);
                assert!(copy_id.is_none());
            }
            _ => panic!("expected File"),
        }
    }

    #[test]
    fn node_file_executable_with_copy_id() {
        let conn = setup();
        let tree = tid(1);
        conn.insert(make_node(
            tree,
            "run.sh",
            TreeValue::File {
                file_id: fid(3),
                executable: true,
                copy_id: Some(cpid(7)),
            },
        ))
        .unwrap();
        let rows = TreeEntry::get_all_for_tree(&conn, &tree).unwrap();
        match rows[0].value {
            TreeValue::File {
                file_id,
                executable,
                copy_id,
            } => {
                assert_eq!(file_id, fid(3));
                assert!(executable);
                assert_eq!(copy_id, Some(cpid(7)));
            }
            _ => panic!("expected File"),
        }
    }

    #[test]
    fn node_symlink() {
        let conn = setup();
        let tree = tid(1);
        conn.insert(make_node(tree, "link", TreeValue::Symlink(sid(4))))
            .unwrap();
        let rows = TreeEntry::get_all_for_tree(&conn, &tree).unwrap();
        assert!(matches!(rows[0].value, TreeValue::Symlink(id) if id == sid(4)));
    }

    #[test]
    fn node_tree() {
        let conn = setup();
        let tree = tid(1);
        conn.insert(make_node(tree, "subdir", TreeValue::Tree(tid(5))))
            .unwrap();
        let rows = TreeEntry::get_all_for_tree(&conn, &tree).unwrap();
        assert!(matches!(rows[0].value, TreeValue::Tree(id) if id == tid(5)));
    }

    #[test]
    fn node_submodule() {
        let conn = setup();
        let tree = tid(1);
        conn.insert(make_node(tree, "sub", TreeValue::Submodule(cid(6))))
            .unwrap();
        let rows = TreeEntry::get_all_for_tree(&conn, &tree).unwrap();
        assert!(matches!(rows[0].value, TreeValue::Submodule(id) if id == cid(6)));
    }

    #[test]
    fn node_get_all_sorted_by_name() {
        let conn = setup();
        let tree = tid(1);
        conn.insert(make_node(tree, "z.txt", TreeValue::Symlink(sid(1))))
            .unwrap();
        conn.insert(make_node(tree, "a.txt", TreeValue::Symlink(sid(2))))
            .unwrap();
        conn.insert(make_node(tree, "m.txt", TreeValue::Symlink(sid(3))))
            .unwrap();
        let rows = TreeEntry::get_all_for_tree(&conn, &tree).unwrap();
        let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["a.txt", "m.txt", "z.txt"]);
    }

    fn make_commit(id: CommitId) -> Commit {
        Commit {
            id,
            change_id: chid(1),
            description: "desc".to_owned(),
            author: Signature {
                name: "Alice".to_owned(),
                email: "alice@example.com".to_owned(),
                timestamp: 1_000_000,
                tz_offset: 0,
            },
            committer: Signature {
                name: "Bob".to_owned(),
                email: "bob@example.com".to_owned(),
                timestamp: 2_000_000,
                tz_offset: 60,
            },
            secure_sig: None,
        }
    }

    #[test]
    fn commit_root_trees_roundtrip() {
        let conn = setup();
        conn.insert(make_commit(cid(1))).unwrap();
        // Flat Merge vec order: add0 at position 0, remove0 at position 1.
        let rows = vec![
            CommitRootTree {
                commit_id: cid(1),
                position: 0,
                tree_id: tid(10),
                conflict_label: String::new(),
            },
            CommitRootTree {
                commit_id: cid(1),
                position: 1,
                tree_id: tid(11),
                conflict_label: "base".to_owned(),
            },
        ];
        for row in rows {
            conn.insert(row).unwrap();
        }
        let got = CommitRootTree::get_all_for_commit(&conn, &cid(1)).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].tree_id, tid(10));
        assert_eq!(got[0].conflict_label, String::new());
        assert_eq!(got[1].tree_id, tid(11));
        assert_eq!(got[1].conflict_label, "base");
    }

    #[test]
    fn commit_parents_roundtrip() {
        let conn = setup();
        conn.insert(make_commit(cid(1))).unwrap();
        conn.insert(CommitParent {
            commit_id: cid(1),
            position: 0,
            parent_id: cid(2),
        })
        .unwrap();
        conn.insert(CommitParent {
            commit_id: cid(1),
            position: 1,
            parent_id: cid(3),
        })
        .unwrap();
        let got = CommitParent::get_all_for_commit(&conn, &cid(1)).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].parent_id, cid(2));
        assert_eq!(got[1].parent_id, cid(3));
    }

    #[test]
    fn commit_predecessors_roundtrip() {
        let conn = setup();
        conn.insert(make_commit(cid(1))).unwrap();
        conn.insert(CommitPredecessor {
            commit_id: cid(1),
            position: 0,
            predecessor_id: cid(4),
        })
        .unwrap();
        let got = CommitPredecessor::get_all_for_commit(&conn, &cid(1)).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].predecessor_id, cid(4));
    }

    #[test]
    fn copy_parents_roundtrip() {
        let conn = setup();
        conn.insert(CopyParent {
            copy_id: cpid(1),
            position: 0,
            parent_id: cpid(2),
        })
        .unwrap();
        conn.insert(CopyParent {
            copy_id: cpid(1),
            position: 1,
            parent_id: cpid(3),
        })
        .unwrap();
        let got = CopyParent::get_all_for_copy(&conn, &cpid(1)).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].parent_id, cpid(2));
        assert_eq!(got[1].parent_id, cpid(3));
    }
}
