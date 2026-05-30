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
const ZEROS: Group = Group::splat(0);
const ONES: Group = Group::splat(1);
const MINUS_ONES: Group = Group::splat(-1);

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
                let bits = Group::splat(((hash >> (g * 8)) as u8) as i16);
                let mask = (bits & BIT_MASKS).simd_ne(ZEROS);
                *group += mask.select(ONES, MINUS_ONES);
            }
        }
    }

    /// Consume the hasher and return the 64-bit SimHash fingerprint.
    pub fn finish(self) -> u64 {
        let mut result = 0u64;
        for (g, group) in self.counters.into_iter().enumerate() {
            result |= group.simd_gt(Group::splat(0)).to_bitmask() << (g * 8);
        }
        result
    }
}

/// Compute a 64-bit SimHash fingerprint of `content` in one shot.
pub fn simhash<const N: usize>(content: &[u8]) -> u64 {
    let mut hasher = SimHasher::<N>::new();
    hasher.update(content);
    hasher.finish()
}

/// Count the number of differing bits between two SimHashes (Hamming distance).
///
/// Lower values indicate higher content similarity. A distance of 0 means
/// identical fingerprints; ≤ 10 is typically a strong similarity signal.
pub fn hamming_distance(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

#[cfg(test)]
mod tests {
    use super::*;

    const N: usize = 8;

    #[test]
    fn empty_content_is_zero() {
        assert_eq!(simhash::<N>(&[]), 0);
    }

    #[test]
    fn identical_content_has_distance_zero() {
        let content = b"the quick brown fox jumps over the lazy dog";
        assert_eq!(
            hamming_distance(simhash::<N>(content), simhash::<N>(content)),
            0
        );
    }

    #[test]
    fn similar_content_has_low_distance() {
        let a = b"the quick brown fox jumps over the lazy dog";
        let b = b"the quick brown fox jumps over the lazy cat";
        let dist = hamming_distance(simhash::<N>(a), simhash::<N>(b));
        assert!(dist < 20, "expected low hamming distance, got {dist}");
    }

    #[test]
    fn different_content_is_non_zero() {
        let a = simhash::<N>(b"hello world, this is some content for testing");
        let b = simhash::<N>(b"completely unrelated bytes 1234567890 abcdefgh");
        assert_ne!(a, b);
    }

    #[test]
    fn hamming_distance_reflexive() {
        let h = simhash::<N>(b"some test content that is long enough to shingle");
        assert_eq!(hamming_distance(h, h), 0);
    }

    #[test]
    fn hamming_distance_symmetric() {
        let a = simhash::<N>(b"first string with enough bytes to be shingled");
        let b = simhash::<N>(b"second string with enough bytes to be shingled");
        assert_eq!(hamming_distance(a, b), hamming_distance(b, a));
    }

    /// Feeding data in one chunk must produce the same result as feeding it
    /// in many small chunks (including chunks smaller than N).
    #[test]
    fn streaming_matches_oneshot() {
        let content = b"the quick brown fox jumps over the lazy dog and other animals";
        let oneshot = simhash::<N>(content);

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
