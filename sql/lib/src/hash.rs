use jj_lib::hex_util::decode_hex;
use jj_lib::hex_util::encode_hex;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash<const N: usize>(pub [u8; N]);

impl<const N: usize> rusqlite::ToSql for Hash<N> {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl<const N: usize> rusqlite::types::FromSql for Hash<N> {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        <[u8; N]>::column_result(value).map(Hash)
    }
}

impl<const N: usize> balsaq::Column for Hash<N> {
    const SQL_TYPE: &'static str = <[u8; N]>::SQL_TYPE;
    const NULLABLE: bool = <[u8; N]>::NULLABLE;
}

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

impl From<blake2::digest::Output<blake2::Blake2b512>> for Hash<64> {
    fn from(value: blake2::digest::Output<blake2::Blake2b512>) -> Self {
        Self(value.into())
    }
}
