//! SimHash over byte shingles for file-content similarity estimation.
//!
//! A SimHash is a 64-bit fingerprint where two similar byte sequences produce
//! fingerprints with a low Hamming distance. It is used to find good delta
//! compression base candidates without comparing all O(N²) pairs of blobs.
//!
//! # Algorithm
//!
//! 1. Slide a window of `N` bytes over the raw byte content using
//!    [`BuzHasher`](crate::buzhash::BuzHasher).
//! 2. Maintain a `[i64; 64]` counter vector: for each bit position, increment
//!    if the corresponding bit of the window hash is 1, decrement if 0.
//! 3. Collapse: bit *i* of the SimHash is 1 if `counters[i] > 0`, else 0.
//!
//! # Short content
//!
//! Content shorter than `N` bytes produces no complete windows. The SimHash
//! is defined to be 0 in that case.

use std::simd::prelude::*;

use crate::buzhash::BuzHasher;

/// One 128-bit NEON register: 8 × i16 counters.
type Group = Simd<i16, 8>;

/// Bit-isolation masks: lane `i` selects bit `i` of the broadcast byte.
const BIT_MASKS: Group = Group::from_array([1, 2, 4, 8, 16, 32, 64, 128]);
const ZEROES: Group = Group::splat(0);
const ONES: Group = Group::splat(1);
const MINUS_ONES: Group = Group::splat(-1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimHash<const N: usize>(pub u64);

/// Streaming SimHash computation over `N`-byte shingles.
///
/// Feed data incrementally with [`update`](SimHasher::update), then call
/// [`finish`](SimHasher::finish) to obtain the 64-bit fingerprint.
pub struct SimHasher<const N: usize> {
    /// 8 groups × 8 lanes = 64 i16 counters, one per hash bit. Overflows
    /// after ~32K windows (~32 KB for N=8), acceptable for a heuristic.
    counters: [Group; 8],
    buz: BuzHasher<N>,
}

impl<const N: usize> SimHasher<N> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            counters: [Group::splat(0); 8],
            buz: BuzHasher::new(),
        }
    }

    /// Feed the next chunk of data into the hasher.
    pub fn update(&mut self, data: &[u8]) {
        for &byte in data {
            let hash = self.buz.push(byte);
            for (g, group) in self.counters.iter_mut().enumerate() {
                // Broadcast one byte of the hash across all 8 lanes, isolate
                // each bit with a mask, then add ±1 based on whether it is set.
                let bits = Group::splat(((hash.0 >> (g * 8)) as u8) as i16);
                let mask = (bits & BIT_MASKS).simd_ne(ZEROES);
                *group += mask.select(ONES, MINUS_ONES);
            }
        }
    }

    /// Consume the hasher and return the 64-bit SimHash fingerprint.
    pub fn finish(self) -> SimHash<N> {
        let mut result = 0u64;
        for (g, group) in self.counters.into_iter().enumerate() {
            result |= group.simd_gt(ZEROES).to_bitmask() << (g * 8);
        }
        SimHash(result)
    }
}

impl<const N: usize> SimHash<N> {
    /// Compute a 64-bit SimHash fingerprint of `content` in one shot.
    pub fn of(content: &[u8]) -> Self {
        let mut hasher = SimHasher::<N>::new();
        hasher.update(content);
        hasher.finish()
    }

    /// Count the number of differing bits between two SimHashes (Hamming
    /// distance).
    ///
    /// Lower values indicate higher content similarity. A distance of 0 means
    /// identical fingerprints; ≤ 10 is typically a strong similarity signal.
    pub fn hamming_distance(self, other: Self) -> u32 {
        (self.0 ^ other.0).count_ones()
    }
}

impl<const N: usize> rusqlite::ToSql for SimHash<N> {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok((self.0 as i64).into())
    }
}

impl<const N: usize> rusqlite::types::FromSql for SimHash<N> {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        i64::column_result(value).map(|h| Self(h as u64))
    }
}

impl<const N: usize> balsaq::Column for SimHash<N> {
    const SQL_TYPE: &'static str = i64::SQL_TYPE;
    const NULLABLE: bool = i64::NULLABLE;
}

#[cfg(test)]
mod tests {
    use super::*;

    const N: usize = 8;

    #[test]
    fn empty_content_is_zero() {
        assert_eq!(SimHash::<N>::of(&[]), SimHash::<N>(0));
    }

    #[test]
    fn identical_content_has_distance_zero() {
        let content = b"the quick brown fox jumps over the lazy dog";
        assert_eq!(
            SimHash::<N>::of(content).hamming_distance(SimHash::<N>::of(content)),
            0
        );
    }

    #[test]
    fn similar_content_has_low_distance() {
        let a = b"the quick brown fox jumps over the lazy dog";
        let b = b"the quick brown fox jumps over the lazy cat";
        let dist = SimHash::<N>::of(a).hamming_distance(SimHash::<N>::of(b));
        assert!(dist < 20, "expected low hamming distance, got {dist}");
    }

    #[test]
    fn different_content_is_non_zero() {
        let a = SimHash::<N>::of(b"hello world, this is some content for testing");
        let b = SimHash::<N>::of(b"completely unrelated bytes 1234567890 abcdefgh");
        assert_ne!(a, b);
    }

    #[test]
    fn hamming_distance_reflexive() {
        let h = SimHash::<N>::of(b"some test content that is long enough to shingle");
        assert_eq!(h.hamming_distance(h), 0);
    }

    #[test]
    fn hamming_distance_symmetric() {
        let a = SimHash::<N>::of(b"first string with enough bytes to be shingled");
        let b = SimHash::<N>::of(b"second string with enough bytes to be shingled");
        assert_eq!(a.hamming_distance(b), b.hamming_distance(a));
    }

    /// Feeding data in one chunk must produce the same result as feeding it
    /// in many small chunks (including chunks smaller than N).
    #[test]
    fn streaming_matches_oneshot() {
        let content = b"the quick brown fox jumps over the lazy dog and other animals";
        let oneshot = SimHash::<N>::of(content);

        let mut hasher = SimHasher::<N>::new();
        for byte in content {
            hasher.update(std::slice::from_ref(byte));
        }
        assert_eq!(hasher.finish(), oneshot, "byte-by-byte streaming mismatch");

        let mut hasher = SimHasher::<N>::new();
        for chunk in content.chunks(3) {
            hasher.update(chunk);
        }
        assert_eq!(hasher.finish(), oneshot, "chunk-3 streaming mismatch");

        let mut hasher = SimHasher::<N>::new();
        for chunk in content.chunks(N + 1) {
            hasher.update(chunk);
        }
        assert_eq!(hasher.finish(), oneshot, "chunk-(N+1) streaming mismatch");
    }
}
