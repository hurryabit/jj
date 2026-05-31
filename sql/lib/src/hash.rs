use jj_lib::hex_util::decode_hex;
use jj_lib::hex_util::encode_hex;
use sqlx::Database;
use sqlx::Sqlite;
use sqlx::error::BoxDynError;

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    proptest_derive::Arbitrary,
    zerocopy::FromBytes,
    zerocopy::Immutable,
    zerocopy::IntoBytes,
)]
#[repr(transparent)]
pub struct Hash<const N: usize>(pub [u8; N]);

impl<const N: usize> serde::Serialize for Hash<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        encode_hex(&self.0).serialize(serializer)
    }
}

impl<'de, const N: usize> serde::Deserialize<'de> for Hash<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let hex = <&str>::deserialize(deserializer)?;
        let Some(bytes) = decode_hex(hex) else {
            return Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(hex),
                &"a valid hex string",
            ));
        };
        let Ok(array) = <[u8; N]>::try_from(bytes) else {
            return Err(serde::de::Error::invalid_length(
                hex.len() / 2,
                &format!("a hex string of length {}", N * 2).as_str(),
            ));
        };
        Ok(Hash(array))
    }
}

impl<'r, const N: usize> sqlx::Decode<'r, Sqlite> for Hash<N> {
    fn decode(value: <Sqlite as Database>::ValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(Self(
            <&[u8] as sqlx::Decode<Sqlite>>::decode(value)?.try_into()?,
        ))
    }
}

impl<'q, const N: usize> sqlx::Encode<'q, Sqlite> for Hash<N> {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as Database>::ArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, BoxDynError> {
        <&[u8] as sqlx::Encode<Sqlite>>::encode_by_ref(&&self.0[..], buf)
    }
}

impl<const N: usize> sqlx::Type<Sqlite> for Hash<N> {
    fn type_info() -> <Sqlite as Database>::TypeInfo {
        <&[u8] as sqlx::Type<Sqlite>>::type_info()
    }
}

impl From<blake2::digest::Output<blake2::Blake2b512>> for Hash<64> {
    fn from(value: blake2::digest::Output<blake2::Blake2b512>) -> Self {
        Self(value.into())
    }
}
