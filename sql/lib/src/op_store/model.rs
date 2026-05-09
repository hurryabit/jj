use std::collections::BTreeMap;

use balsaq::ConnectionExt as _;
use balsaq::Model as _;
use rusqlite::Connection;
use serde::Deserialize;
use serde::Serialize;

use crate::backend::model::CommitId;
use crate::id_newtype;
use crate::str_newtype;

pub const VIEW_ID_LENGTH: usize = 64;
pub const OPERATION_ID_LENGTH: usize = 64;

id_newtype!(ViewId, VIEW_ID_LENGTH, true);
id_newtype!(OperationId, OPERATION_ID_LENGTH, true);

#[balsaq::schema]
mod schema {
    use super::*;

    #[balsaq::table("views", track_last_update)]
    pub struct ViewRow {
        #[primary_key]
        pub id: ViewId,
        pub data: String,
    }

    #[balsaq::table("operations", track_last_update)]
    pub struct OperationRow {
        #[primary_key]
        pub id: OperationId,
        pub view_id: ViewId,
        pub metadata: String, // JSON-serialized `OperationMetadata`.
        /// JSON-serialised `BTreeMap<CommitId, Vec<CommitId>>`, or NULL if
        /// commit predecessors were not recorded for this operation.
        pub commit_predecessors: Option<String>,
    }

    #[balsaq::table("operation_parents")]
    pub struct OperationParent {
        #[primary_key]
        pub operation_id: OperationId,
        #[primary_key]
        pub position: i64,
        pub parent_id: OperationId,
    }

    impl OperationParent {
        pub fn get_all_for_operation(
            conn: &Connection,
            operation_id: &OperationId,
        ) -> rusqlite::Result<Vec<Self>> {
            const SQL: &str = const_format::concatcp!(
                OperationParent::SELECT,
                " WHERE operation_id = ?1 ORDER BY position ASC"
            );
            conn.get_all(SQL, (operation_id,))
        }
    }
}

pub use schema::*;

#[derive(Serialize, Deserialize)]
pub struct RefTarget {
    /// Merge terms in flat vec order: [add0, remove0, add1, remove1, …].
    pub terms: Vec<Option<CommitId>>,
}

#[derive(Serialize, Deserialize)]
pub struct RemoteRef {
    pub target: RefTarget,
    pub tracked: bool,
}

str_newtype!(RefName);
str_newtype!(RemoteName);
str_newtype!(GitRefName);
str_newtype!(WorkspaceName);

#[derive(Serialize, Deserialize)]
pub struct RemoteView {
    pub bookmarks: BTreeMap<RefName, RemoteRef>,
    pub tags: BTreeMap<RefName, RemoteRef>,
}

#[derive(Serialize, Deserialize)]
pub struct View {
    /// Hex commit IDs of all visible heads.
    pub head_ids: Vec<CommitId>,
    pub local_bookmarks: BTreeMap<RefName, RefTarget>,
    pub local_tags: BTreeMap<RefName, RefTarget>,
    pub remote_views: BTreeMap<RemoteName, RemoteView>,
    pub git_refs: BTreeMap<GitRefName, RefTarget>,
    pub git_head: RefTarget,
    pub wc_commit_ids: BTreeMap<WorkspaceName, CommitId>,
}

#[derive(Serialize, Deserialize)]
pub struct Timestamp {
    pub ms: i64,
    pub tz: i32,
}

#[derive(Serialize, Deserialize)]
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

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;

    use super::*;

    #[test]
    fn schema() {
        assert_snapshot!(SCHEMA);
    }
}
