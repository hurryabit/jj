//! SimHash over byte shingles for file-content similarity estimation.
//!
//! A SimHash is a 64-bit fingerprint where two similar byte sequences produce
//! fingerprints with a low Hamming distance. It is used to find good delta
//! compression base candidates without comparing all O(N²) pairs of blobs.
//!
//! # Algorithm
//!
//! 1. Slide a fixed-size window (shingle) over the raw byte content.
//! 2. Hash each shingle with xxh3 (fast, hardware-accelerated, good avalanche).
//! 3. Maintain a `[i64; 64]` counter vector: for each bit position, increment
//!    if the corresponding bit of the shingle hash is 1, decrement if 0.
//! 4. Collapse: bit *i* of the SimHash is 1 if `counters[i] > 0`, else 0.
//!
//! The shingle size is chosen to capture enough context so that adjacent
//! shingles are not trivially similar, while remaining small enough that even
//! short files produce many shingles.
//!
//! # Empty and very short content
//!
//! Content shorter than `SHINGLE_SIZE` produces no shingles. The SimHash is
//! defined to be 0 in that case, which is a valid (if imprecise) fingerprint.

use xxhash_rust::xxh3::xxh3_64;

/// Number of bytes in each shingle (sliding window).
const SHINGLE_SIZE: usize = 8;

/// Streaming SimHash computation over byte shingles.
///
/// Feed data incrementally with [`update`](SimHasher::update), then call
/// [`finish`](SimHasher::finish) to obtain the 64-bit fingerprint. This
/// mirrors the `Digest` API (e.g. `Blake2b512`) and avoids buffering the
/// entire content in memory.
///
/// Shingles that span chunk boundaries are handled transparently via an
/// internal tail buffer of at most `SHINGLE_SIZE - 1` bytes.
pub struct SimHasher {
    counters: [i64; 64],
    /// Carry-over bytes from the previous `update` call that could not yet
    /// complete a shingle. Always shorter than `SHINGLE_SIZE`.
    tail: [u8; SHINGLE_SIZE - 1],
    tail_len: usize,
}

impl SimHasher {
    pub fn new() -> Self {
        Self {
            counters: [0; 64],
            tail: [0; SHINGLE_SIZE - 1],
            tail_len: 0,
        }
    }

    /// Feed the next chunk of data into the hasher.
    pub fn update(&mut self, data: &[u8]) {
        let combined = self.tail_len + data.len();
        if combined < SHINGLE_SIZE {
            self.tail[self.tail_len..combined].copy_from_slice(data);
            self.tail_len = combined;
            return;
        }

        // Build a boundary buffer: old tail ++ first (SHINGLE_SIZE - 1) bytes
        // of data. Every shingle that spans the tail/data boundary is a window
        // in this buffer; every other shingle is entirely within data.
        let prefix_len = data.len().min(SHINGLE_SIZE - 1);
        let mut boundary = [0u8; 2 * (SHINGLE_SIZE - 1)];
        boundary[..self.tail_len].copy_from_slice(&self.tail[..self.tail_len]);
        boundary[self.tail_len..self.tail_len + prefix_len].copy_from_slice(&data[..prefix_len]);
        let boundary = &boundary[..self.tail_len + prefix_len];

        for window in boundary
            .windows(SHINGLE_SIZE)
            .chain(data.windows(SHINGLE_SIZE))
        {
            let hash = xxh3_64(window);
            for (i, counter) in self.counters.iter_mut().enumerate() {
                if hash & (1u64 << i) != 0 {
                    *counter += 1;
                } else {
                    *counter -= 1;
                }
            }
        }

        let tail_src = if data.len() >= SHINGLE_SIZE - 1 {
            data
        } else {
            boundary
        };
        self.tail_len = SHINGLE_SIZE - 1;
        self.tail[..].copy_from_slice(&tail_src[tail_src.len() - self.tail_len..]);
    }

    /// Consume the hasher and return the 64-bit SimHash fingerprint.
    pub fn finish(self) -> u64 {
        let mut result = 0u64;
        for (i, &counter) in self.counters.iter().enumerate() {
            if counter > 0 {
                result |= 1u64 << i;
            }
        }
        result
    }
}

/// Compute a 64-bit SimHash fingerprint of `content` in one shot.
#[allow(dead_code)]
///
/// Equivalent to creating a [`SimHasher`], calling `update(content)`, and
/// then `finish()`. Prefer [`SimHasher`] when content arrives in chunks.
pub fn simhash(content: &[u8]) -> u64 {
    let mut hasher = SimHasher::new();
    hasher.update(content);
    hasher.finish()
}

/// Count the number of differing bits between two SimHashes (Hamming distance).
#[allow(dead_code)]
///
/// Lower values indicate higher content similarity. A distance of 0 means
/// identical fingerprints; ≤ 10 is typically a strong similarity signal.
pub fn hamming_distance(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_content_is_zero() {
        assert_eq!(simhash(&[]), 0);
    }

    #[test]
    fn short_content_below_shingle_size_is_zero() {
        assert_eq!(simhash(&[1, 2, 3]), 0);
    }

    #[test]
    fn identical_content_has_distance_zero() {
        let content = b"the quick brown fox jumps over the lazy dog";
        assert_eq!(hamming_distance(simhash(content), simhash(content)), 0);
    }

    #[test]
    fn similar_content_has_low_distance() {
        let a = b"the quick brown fox jumps over the lazy dog";
        let b = b"the quick brown fox jumps over the lazy cat";
        let dist = hamming_distance(simhash(a), simhash(b));
        assert!(dist < 20, "expected low hamming distance, got {dist}");
    }

    #[test]
    fn different_content_is_non_zero() {
        let a = simhash(b"hello world, this is some content for testing");
        let b = simhash(b"completely unrelated bytes 1234567890 abcdefgh");
        assert_ne!(a, b);
    }

    #[test]
    fn hamming_distance_reflexive() {
        let h = simhash(b"some test content that is long enough to shingle");
        assert_eq!(hamming_distance(h, h), 0);
    }

    #[test]
    fn hamming_distance_symmetric() {
        let a = simhash(b"first string with enough bytes to be shingled");
        let b = simhash(b"second string with enough bytes to be shingled");
        assert_eq!(hamming_distance(a, b), hamming_distance(b, a));
    }

    /// Feeding data in one chunk must produce the same result as feeding it
    /// in many small chunks (including chunks smaller than SHINGLE_SIZE).
    #[test]
    fn streaming_matches_oneshot() {
        let content = b"the quick brown fox jumps over the lazy dog and other animals";

        let oneshot = simhash(content);

        // Feed one byte at a time.
        let mut hasher = SimHasher::new();
        for byte in content {
            hasher.update(std::slice::from_ref(byte));
        }
        assert_eq!(hasher.finish(), oneshot, "byte-by-byte streaming mismatch");

        // Feed in chunks of 3 (deliberately straddles shingle boundaries).
        let mut hasher = SimHasher::new();
        for chunk in content.chunks(3) {
            hasher.update(chunk);
        }
        assert_eq!(hasher.finish(), oneshot, "chunk-3 streaming mismatch");

        // Feed in chunks of SHINGLE_SIZE + 1 = 9.
        let mut hasher = SimHasher::new();
        for chunk in content.chunks(SHINGLE_SIZE + 1) {
            hasher.update(chunk);
        }
        assert_eq!(hasher.finish(), oneshot, "chunk-9 streaming mismatch");
    }
}
