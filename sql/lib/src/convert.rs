use std::collections::BTreeMap;

use crate::error::SqlBackendError;

pub(crate) trait JjExt {
    type Model: ModelExt<Jj = Self>;
    fn to_model(&self) -> Result<Self::Model, SqlBackendError>;
    fn from_model(model: Self::Model) -> Self;
}

pub(crate) trait ModelExt {
    type Jj: JjExt<Model = Self>;
    fn into_jj(self) -> Self::Jj
    where
        Self: Sized,
    {
        Self::Jj::from_model(self)
    }
}

impl<J: JjExt> JjExt for Vec<J> {
    type Model = Vec<J::Model>;

    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        self.iter().map(|v| v.to_model()).collect()
    }

    fn from_model(model: Self::Model) -> Self {
        model.into_iter().map(J::from_model).collect()
    }
}

impl<M: ModelExt> ModelExt for Vec<M> {
    type Jj = Vec<M::Jj>;
}

impl<J: JjExt> JjExt for Option<J> {
    type Model = Option<J::Model>;

    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        self.as_ref().map(|v| v.to_model()).transpose()
    }

    fn from_model(model: Self::Model) -> Self {
        model.map(J::from_model)
    }
}

impl<M: ModelExt> ModelExt for Option<M> {
    type Jj = Option<M::Jj>;
}

impl<K, V> JjExt for BTreeMap<K, V>
where
    K: JjExt + Ord,
    K::Model: Ord,
    V: JjExt,
{
    type Model = BTreeMap<K::Model, V::Model>;

    fn to_model(&self) -> Result<Self::Model, SqlBackendError> {
        self.iter()
            .map(|(k, v)| Ok((k.to_model()?, v.to_model()?)))
            .collect()
    }

    fn from_model(model: Self::Model) -> Self {
        model
            .into_iter()
            .map(|(k, v)| (K::from_model(k), V::from_model(v)))
            .collect()
    }
}

impl<K, V> ModelExt for BTreeMap<K, V>
where
    K: ModelExt + Ord,
    K::Jj: Ord,
    V: ModelExt,
{
    type Jj = BTreeMap<K::Jj, V::Jj>;
}

#[macro_export]
macro_rules! impl_id_ext {
    ($jj_id:ty, $model_id:path) => {
        impl JjExt for $jj_id {
            type Model = $model_id;
            fn to_model(&self) -> Result<Self::Model, $crate::error::SqlBackendError> {
                let bytes = self.as_bytes();
                let array =
                    bytes
                        .try_into()
                        .map_err(|_err| SqlBackendError::InvalidHashLength {
                            expected: std::mem::size_of::<$model_id>(),
                            actual: bytes.len(),
                            object_type: self.object_type(),
                            hash: jj_lib::hex_util::encode_hex(bytes),
                        })?;

                Ok($model_id($crate::hash::Hash(array)))
            }
            fn from_model(model: Self::Model) -> Self {
                Self::new(model.0.0.to_vec())
            }
        }
        impl ModelExt for $model_id {
            type Jj = $jj_id;
        }
    };
}

/// Implements [`JjExt`] and [`ModelExt`] for a `XxxBuf` / `model::XxxName` pair
/// where the model newtype wraps a plain [`String`].
#[macro_export]
macro_rules! impl_str_ext {
    ($jj_buf:ty, $model_name:path) => {
        impl JjExt for $jj_buf {
            type Model = $model_name;
            fn to_model(&self) -> Result<Self::Model, $crate::error::SqlBackendError> {
                Ok($model_name(self.as_str().to_owned()))
            }
            fn from_model(model: Self::Model) -> Self {
                Self::from(model.0)
            }
        }
        impl ModelExt for $model_name {
            type Jj = $jj_buf;
        }
    };
}
