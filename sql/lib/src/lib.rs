#![feature(portable_simd)]

mod backend;
mod buzhash;
mod convert;
mod error;
mod hash;
mod op_heads_store;
mod op_store;
mod simhash;

pub use crate::backend::DbTableStats;
pub use crate::backend::SqlBackend;
pub use crate::backend::Stats;
pub use crate::backend::model;
pub use crate::buzhash::BuzHasher;
pub use crate::error::SqlBackendError;
pub use crate::op_heads_store::SqlOpHeadsStore;
pub use crate::op_store::SqlOpStore;
pub use crate::simhash::SimHash;
pub use crate::simhash::SimHasher;

/// Defines a hash-backed ID newtype for use as a model primary key.
#[macro_export]
macro_rules! id_newtype {
    ($name:ident, $len:expr, true) => {
        id_newtype!(@impl $name, $len);

        impl From<blake2::digest::Output<blake2::Blake2b512>> for $name {
            fn from(value: blake2::digest::Output<blake2::Blake2b512>) -> Self {
                Self($crate::hash::Hash::<$len>::from(value))
            }
        }
    };

    ($name:ident, $len:expr, false) => {
        id_newtype!(@impl $name, $len);
    };

    (@impl $name:ident, $len:expr) => {
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash,
            balsaq::Column,
            serde::Deserialize, serde::Serialize,
            zerocopy::IntoBytes, zerocopy::Immutable,
        )]
        #[repr(transparent)]
        #[serde(transparent)]
        pub struct $name(pub $crate::hash::Hash<$len>);
    };
}

/// Defines a serde-transparent string newtype for use as a model key.
#[macro_export]
macro_rules! str_newtype {
    ($name:ident) => {
        #[derive(
            Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash,
            serde::Deserialize, serde::Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub String);
    };
}
