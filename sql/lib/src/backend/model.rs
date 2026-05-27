use balsaq::ConnectionExt as _;
use balsaq::Model as _;
use rusqlite::Connection;
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

    #[balsaq::table("files", auto_primary_key, track_last_update)]
    pub struct File {
        #[unique]
        pub id: FileId,
        pub content: Vec<u8>,
        pub uncompressed_size: i64,
        /// SimHash fingerprint of the uncompressed content, used to find
        /// delta-compression base candidates by Hamming distance.
        pub simhash: i64,
    }

    #[balsaq::table("symlinks", auto_primary_key, track_last_update)]
    pub struct Symlink {
        #[unique]
        pub id: SymlinkId,
        pub target: String,
    }

    #[balsaq::table("trees", auto_primary_key, track_last_update)]
    pub struct Tree {
        #[unique]
        pub id: TreeId,
        pub entries: Vec<u8>,
    }

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum TreeEntryValue {
        File {
            row_id: i64,
            executable: bool,
            copy_id: Option<CopyId>,
        },
        Symlink(i64),
        Tree(i64),
        Submodule(i64),
    }

    #[balsaq::table("commits", auto_primary_key, track_last_update)]
    #[index(change_id)]
    pub struct Commit {
        #[unique]
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

pub type TreeEntries = Vec<(String, TreeEntryValue)>;

pub fn encode_tree_entries(entries: &TreeEntries) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(entries)
}

pub fn decode_tree_entries(bytes: &[u8]) -> Result<TreeEntries, postcard::Error> {
    postcard::from_bytes(bytes)
}

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

    fn insert_file(conn: &Connection, id: FileId) -> FileRowId {
        conn.insert(File {
            id,
            content: vec![],
            uncompressed_size: 0,
            simhash: 0,
        })
        .unwrap()
    }

    fn insert_symlink(conn: &Connection, id: SymlinkId) -> SymlinkRowId {
        conn.insert(Symlink {
            id,
            target: "target".to_owned(),
        })
        .unwrap()
    }

    fn insert_tree(conn: &Connection, id: TreeId, entries: TreeEntries) -> TreeRowId {
        let blob = encode_tree_entries(&entries).unwrap();
        conn.insert(Tree { id, entries: blob }).unwrap()
    }

    #[test]
    fn tree_roundtrip() {
        let conn = setup();
        insert_tree(&conn, tid(3), TreeEntries::new());
        assert!(Tree::get_by_id(&conn, &tid(3)).is_ok());
        assert!(Tree::get_by_id(&conn, &tid(99)).is_err());
    }

    #[test]
    fn tree_entries_roundtrip() {
        let conn = setup();
        let f_row = insert_file(&conn, fid(2));
        let s_row = insert_symlink(&conn, sid(4));
        let sub_row = insert_tree(&conn, tid(5), TreeEntries::new());
        let entries = vec![
            (
                "a.txt".to_owned(),
                TreeEntryValue::File {
                    row_id: f_row.0,
                    executable: false,
                    copy_id: None,
                },
            ),
            (
                "link".to_owned(),
                TreeEntryValue::Symlink(s_row.0),
            ),
            (
                "subdir".to_owned(),
                TreeEntryValue::Tree(sub_row.0),
            ),
        ];
        insert_tree(&conn, tid(1), entries.clone());
        let (_, tree) = Tree::get_by_id(&conn, &tid(1)).unwrap();
        let decoded = decode_tree_entries(&tree.entries).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn tree_entries_all_variants() {
        let conn = setup();
        let f_row = insert_file(&conn, fid(2));
        let s_row = insert_symlink(&conn, sid(4));
        let sub_row = insert_tree(&conn, tid(5), TreeEntries::new());
        let c_row = conn.insert(make_commit(cid(6))).unwrap();

        let entries = vec![
            (
                "exec.sh".to_owned(),
                TreeEntryValue::File {
                    row_id: f_row.0,
                    executable: true,
                    copy_id: Some(cpid(9)),
                },
            ),
            ("link".to_owned(), TreeEntryValue::Symlink(s_row.0)),
            ("subdir".to_owned(), TreeEntryValue::Tree(sub_row.0)),
            ("sub".to_owned(), TreeEntryValue::Submodule(c_row.0)),
        ];
        let blob = encode_tree_entries(&entries).unwrap();
        let decoded = decode_tree_entries(&blob).unwrap();
        assert_eq!(decoded, entries);
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
