use jj_lib::backend;
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store as jj;
use jj_lib::op_store::OperationId;
use jj_lib::op_store::RemoteRefState;
use jj_lib::op_store::TimestampRange;
use jj_lib::op_store::ViewId;
use jj_lib::ref_name::GitRefNameBuf;
use jj_lib::ref_name::RefNameBuf;
use jj_lib::ref_name::RemoteNameBuf;
use jj_lib::ref_name::WorkspaceNameBuf;

use super::model;
use crate::convert::JjExt;
use crate::convert::ModelExt;
use crate::error::SqlBackendError;
use crate::impl_id_ext;
use crate::impl_str_ext;

impl_str_ext!(RefNameBuf, model::RefName);
impl_str_ext!(RemoteNameBuf, model::RemoteName);
impl_str_ext!(GitRefNameBuf, model::GitRefName);
impl_str_ext!(WorkspaceNameBuf, model::WorkspaceName);

impl_id_ext!(ViewId, model::ViewId);
impl_id_ext!(OperationId, model::OperationId);

impl JjExt for backend::Timestamp {
    type Model = model::Timestamp;
    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        Ok(model::Timestamp {
            ms: self.timestamp.0,
            tz: self.tz_offset,
        })
    }
    fn from_model(model: Self::Model) -> Self {
        backend::Timestamp {
            timestamp: backend::MillisSinceEpoch(model.ms),
            tz_offset: model.tz,
        }
    }
}
impl ModelExt for model::Timestamp {
    type Jj = backend::Timestamp;
}

impl JjExt for jj::RefTarget {
    type Model = model::RefTarget;
    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        Ok(model::RefTarget {
            terms: self
                .as_merge()
                .iter()
                .map(|opt| opt.as_ref().map(|id| id.to_model()).transpose())
                .collect::<Result<_, SqlBackendError>>()?,
        })
    }
    fn from_model(model: Self::Model) -> Self {
        let terms: Vec<_> = model
            .terms
            .into_iter()
            .map(|opt| opt.map(|id| id.into_jj()))
            .collect();
        jj::RefTarget::from_merge(Merge::from_vec(terms))
    }
}
impl ModelExt for model::RefTarget {
    type Jj = jj::RefTarget;
}

impl JjExt for jj::RemoteRef {
    type Model = model::RemoteRef;
    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        Ok(model::RemoteRef {
            target: self.target.to_model()?,
            tracked: self.state == RemoteRefState::Tracked,
        })
    }
    fn from_model(model: Self::Model) -> Self {
        jj::RemoteRef {
            target: model.target.into_jj(),
            state: if model.tracked {
                RemoteRefState::Tracked
            } else {
                RemoteRefState::New
            },
        }
    }
}
impl ModelExt for model::RemoteRef {
    type Jj = jj::RemoteRef;
}

impl JjExt for jj::RemoteView {
    type Model = model::RemoteView;
    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        Ok(model::RemoteView {
            bookmarks: self.bookmarks.to_model()?,
            tags: self.tags.to_model()?,
        })
    }
    fn from_model(model: Self::Model) -> Self {
        jj::RemoteView {
            bookmarks: model.bookmarks.into_jj(),
            tags: model.tags.into_jj(),
        }
    }
}
impl ModelExt for model::RemoteView {
    type Jj = jj::RemoteView;
}

impl JjExt for jj::View {
    type Model = model::View;
    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        Ok(model::View {
            head_ids: self
                .head_ids
                .iter()
                .map(|id| id.to_model())
                .collect::<Result<_, SqlBackendError>>()?,
            local_bookmarks: self.local_bookmarks.to_model()?,
            local_tags: self.local_tags.to_model()?,
            remote_views: self.remote_views.to_model()?,
            git_refs: self.git_refs.to_model()?,
            git_head: self.git_head.to_model()?,
            wc_commit_ids: self.wc_commit_ids.to_model()?,
        })
    }
    fn from_model(model: Self::Model) -> Self {
        jj::View {
            head_ids: model.head_ids.into_iter().map(|id| id.into_jj()).collect(),
            local_bookmarks: model.local_bookmarks.into_jj(),
            local_tags: model.local_tags.into_jj(),
            remote_views: model.remote_views.into_jj(),
            git_refs: model.git_refs.into_jj(),
            git_head: model.git_head.into_jj(),
            wc_commit_ids: model.wc_commit_ids.into_jj(),
        }
    }
}
impl ModelExt for model::View {
    type Jj = jj::View;
}

impl JjExt for jj::OperationMetadata {
    type Model = model::OperationMetadata;
    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        Ok(model::OperationMetadata {
            time_start: self.time.start.to_model()?,
            time_end: self.time.end.to_model()?,
            description: self.description.clone(),
            hostname: self.hostname.clone(),
            username: self.username.clone(),
            is_snapshot: self.is_snapshot,
            workspace_name: self.workspace_name.as_ref().map(|n| n.as_str().to_owned()),
            attributes: self.attributes.clone(),
        })
    }
    fn from_model(model: Self::Model) -> Self {
        jj::OperationMetadata {
            time: TimestampRange {
                start: model.time_start.into_jj(),
                end: model.time_end.into_jj(),
            },
            description: model.description,
            hostname: model.hostname,
            username: model.username,
            is_snapshot: model.is_snapshot,
            workspace_name: model.workspace_name.map(WorkspaceNameBuf::from),
            attributes: model.attributes,
        }
    }
}
impl ModelExt for model::OperationMetadata {
    type Jj = jj::OperationMetadata;
}
