use jj_lib::backend::ChangeId;
use jj_lib::backend::CommitId;
use jj_lib::backend::CopyId;
use jj_lib::backend::FileId;
use jj_lib::backend::MillisSinceEpoch;
use jj_lib::backend::SecureSig;
use jj_lib::backend::Signature;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Timestamp;
use jj_lib::backend::TreeId;
use jj_lib::object_id::ObjectId as _;

use super::model;
use crate::convert::JjExt;
use crate::convert::ModelExt;
use crate::error::SqlBackendError;
use crate::impl_id_ext;

impl_id_ext!(FileId, model::FileId);
impl_id_ext!(SymlinkId, model::SymlinkId);
impl_id_ext!(TreeId, model::TreeId);
impl_id_ext!(CommitId, model::CommitId);
impl_id_ext!(CopyId, model::CopyId);
impl_id_ext!(ChangeId, model::ChangeId);

impl JjExt for Signature {
    type Model = model::Signature;
    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        Ok(model::Signature {
            name: self.name.clone(),
            email: self.email.clone(),
            timestamp: self.timestamp.timestamp.0,
            tz_offset: self.timestamp.tz_offset,
        })
    }
    fn from_model(model: Self::Model) -> Self {
        Signature {
            name: model.name,
            email: model.email,
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(model.timestamp),
                tz_offset: model.tz_offset,
            },
        }
    }
}
impl ModelExt for model::Signature {
    type Jj = Signature;
}

impl JjExt for SecureSig {
    type Model = model::SecureSig;
    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        Ok(model::SecureSig {
            data: self.data.clone(),
            sig: self.sig.clone(),
        })
    }
    fn from_model(model: Self::Model) -> Self {
        SecureSig {
            data: model.data,
            sig: model.sig,
        }
    }
}
impl ModelExt for model::SecureSig {
    type Jj = SecureSig;
}

