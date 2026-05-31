use std::marker::PhantomData;

use sqlx::Database;
use sqlx::Sqlite;
use sqlx::error::BoxDynError;

use crate::SqlBackendError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Postcard<T>(Vec<u8>, PhantomData<T>);

impl<T: serde::Serialize> Postcard<T> {
    pub fn encode(value: &T) -> Result<Self, SqlBackendError> {
        Ok(Self(postcard::to_allocvec(&value)?, PhantomData))
    }
}

impl<'de, T: serde::Deserialize<'de>> Postcard<T> {
    pub fn decode(&'de self) -> Result<T, SqlBackendError> {
        Ok(postcard::from_bytes(&self.0)?)
    }
}

impl<T> sqlx::Type<Sqlite> for Postcard<T> {
    fn type_info() -> <Sqlite as Database>::TypeInfo {
        <Vec<u8> as sqlx::Type<Sqlite>>::type_info()
    }
}

impl<'q, T> sqlx::Encode<'q, Sqlite> for Postcard<T> {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as Database>::ArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, BoxDynError> {
        <Vec<u8> as sqlx::Encode<Sqlite>>::encode_by_ref(&self.0, buf)
    }
}

impl<'r, T> sqlx::Decode<'r, Sqlite> for Postcard<T> {
    fn decode(value: <Sqlite as Database>::ValueRef<'r>) -> Result<Self, BoxDynError> {
        let bytes = <Vec<u8> as sqlx::Decode<Sqlite>>::decode(value)?;
        Ok(Self(bytes, PhantomData))
    }
}

impl<T> proptest::arbitrary::Arbitrary for Postcard<T>
where
    T: proptest::arbitrary::Arbitrary + serde::Serialize,
{
    type Parameters = T::Parameters;
    type Strategy = proptest::strategy::Map<T::Strategy, fn(T) -> Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        use proptest::strategy::Strategy as _;
        T::arbitrary_with(args).prop_map(
            (|v| Self::encode(&v).expect("postcard encoding should not fail")) as fn(T) -> Self,
        )
    }
}
