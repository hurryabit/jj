#![feature(portable_simd)]

pub mod backend;
mod buzhash;
mod convert;
mod error;
mod hash;
mod model;
mod op_heads_store;
mod op_store;
mod postcard;
mod simhash;

pub use crate::backend::DbTableStats;
pub use crate::backend::FilesStats;
pub use crate::backend::SqlBackend;
pub use crate::buzhash::BuzHasher;
pub use crate::error::SqlBackendError;
pub use crate::op_heads_store::SqlOpHeadsStore;
pub use crate::op_store::SqlOpStore;
pub use crate::simhash::SimHash;
pub use crate::simhash::SimHasher;

pub trait Id<T>:
    Copy
    + std::fmt::Debug
    + Eq
    + std::hash::Hash
    + From<T>
    + Into<T>
    + Send
    + Unpin
    + zerocopy::Immutable
    + zerocopy::IntoBytes
where
    T: Copy
        + std::fmt::Debug
        + std::hash::Hash
        + Send
        + Unpin
        + zerocopy::Immutable
        + zerocopy::IntoBytes,
{
}

#[macro_export]
macro_rules! row_id_newtype {
    ($name: ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            ::proptest_derive::Arbitrary,
            ::serde::Deserialize,
            ::serde::Serialize,
            ::sqlx::Type,
            ::zerocopy::FromBytes,
            ::zerocopy::Immutable,
            ::zerocopy::IntoBytes,
        )]
        #[repr(transparent)]
        #[serde(transparent)]
        #[sqlx(transparent)]
        pub struct $name(pub i64);

        impl ::std::convert::From<i64> for $name {
            fn from(value: i64) -> Self {
                Self(value)
            }
        }

        impl ::std::convert::From<$name> for i64 {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl $crate::Id<i64> for $name {}
    };
}

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
            ::proptest_derive::Arbitrary,
            ::serde::Deserialize, ::serde::Serialize,
            ::sqlx::Type,
            ::zerocopy::FromBytes, ::zerocopy::Immutable, ::zerocopy::IntoBytes,
        )]
        #[repr(transparent)]
        #[serde(transparent)]
        #[sqlx(transparent)]
        pub struct $name(pub $crate::hash::Hash<$len>);

        impl $name {
            pub const ZERO: Self = Self($crate::hash::Hash([0; _]));
        }

        impl ::std::convert::From<$crate::hash::Hash<$len>> for $name {
            fn from(value: $crate::hash::Hash<$len>) -> Self {
                Self(value)
            }
        }

        impl ::std::convert::From<$name> for $crate::hash::Hash<$len> {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl $crate::Id<$crate::hash::Hash<$len>> for $name {}
    };
}

/// Defines a serde-transparent string newtype for use as a model key.
#[macro_export]
macro_rules! str_newtype {
    ($name:ident) => {
        #[derive(
            Clone,
            Debug,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            ::proptest_derive::Arbitrary,
            serde::Deserialize,
            serde::Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub String);
    };
}
