use jj_lib::backend as jj;
use jj_lib::object_id::ObjectId as _;

use crate::backend::model::ChangeId;
use crate::backend::model::CommitId;
use crate::backend::model::CopyId;
use crate::backend::model::FileId;
use crate::backend::model::SecureSig;
use crate::backend::model::Signature;
use crate::backend::model::SymlinkId;
use crate::backend::model::TreeId;
use crate::convert::JjExt;
use crate::convert::ModelExt;
use crate::error::SqlBackendError;
use crate::impl_id_ext;

impl_id_ext!(jj::FileId, FileId);
impl_id_ext!(jj::SymlinkId, SymlinkId);
impl_id_ext!(jj::TreeId, TreeId);
impl_id_ext!(jj::CommitId, CommitId);
impl_id_ext!(jj::CopyId, CopyId);
impl_id_ext!(jj::ChangeId, ChangeId);

impl JjExt for jj::Signature {
    type Model = Signature;
    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        Ok(Signature {
            name: self.name.clone(),
            email: self.email.clone(),
            timestamp: self.timestamp.timestamp.0,
            tz_offset: self.timestamp.tz_offset,
        })
    }
    fn from_model(model: Self::Model) -> Self {
        Self {
            name: model.name,
            email: model.email,
            timestamp: jj::Timestamp {
                timestamp: jj::MillisSinceEpoch(model.timestamp),
                tz_offset: model.tz_offset,
            },
        }
    }
}

impl ModelExt for Signature {
    type Jj = jj::Signature;
}

impl JjExt for jj::SecureSig {
    type Model = SecureSig;
    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        Ok(SecureSig {
            data: self.data.clone(),
            sig: self.sig.clone(),
        })
    }
    fn from_model(model: Self::Model) -> Self {
        Self {
            data: model.data,
            sig: model.sig,
        }
    }
}
impl ModelExt for SecureSig {
    type Jj = jj::SecureSig;
}
