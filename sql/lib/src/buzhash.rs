//! Buzhash rolling hash for sliding-window shingling.
//!
//! The window hash updates in O(1) per byte:
//! ```text
//! new_hash = rotate_left(old_hash, 1)
//!          ^ rotate_left(TABLE[outgoing_byte], N)
//!          ^ TABLE[incoming_byte]
//! ```
//!
//! The window is initialised to all zeros, so the first `N` hashes carry a
//! fixed bias from those phantom zero bytes. The bias at position `k` is
//! identical for every hasher, so two hashers that have processed the same
//! number of bytes and share the same last `N` bytes always produce the same
//! hash.

/// Generate the lookup table at compile time from a seeded xorshift64 PRNG.
const fn generate_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut state: u64 = 0x123456789ABCDEF0;
    let mut i = 0usize;
    while i < 256 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        table[i] = state;
        i += 1;
    }
    table
}

/// One 64-bit constant per byte value.
///
/// IMPORTANT: these values must never change once any hash computed from this
/// table has been persisted — two hashes are only comparable if they were
/// produced with the same table.
///
/// This is a placeholder derived from a seeded xorshift64 PRNG. Replace it
/// with truly random values (generated offline) before storing hashes.
const TABLE: [u64; 256] = generate_table();

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct BuzHash<const N: usize>(pub u64);

/// Buzhash rolling hasher over a sliding window of `N` bytes.
///
/// Call [`push`](BuzHasher::push) for each incoming byte. The window is
/// zero-initialised, so every call returns a hash immediately.
pub struct BuzHasher<const N: usize> {
    hash: u64,
    window: [u8; N], // ring buffer
    pos: usize,      // position for the next byte (and of the oldest byte)
}

impl<const N: usize> BuzHasher<N> {
    const HASH_INIT: u64 = {
        let mut hash: u64 = 0;
        let mut i: usize = 0;
        while i < N {
            hash = hash.rotate_left(1) ^ TABLE[0];
            i += 1;
        }
        hash
    };

    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        const {
            assert!(N >= 1, "BuzHasher window size N must at least one");
            assert!(
                N <= u32::MAX as usize,
                "BuzHasher window size N must fit in a u32"
            );
        }
        Self {
            hash: Self::HASH_INIT,
            window: [0; N],
            pos: 0,
        }
    }

    pub fn push(&mut self, byte: u8) -> BuzHash<N> {
        let outgoing = self.window[self.pos];
        self.hash = self.hash.rotate_left(1)
            ^ TABLE[byte as usize]
            ^ TABLE[outgoing as usize].rotate_left(N as u32);
        self.window[self.pos] = byte;
        self.pos = (self.pos + 1) % N;
        BuzHash(self.hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const N: usize = 8;

    fn buzhashes(data: &[u8]) -> Vec<BuzHash<N>> {
        let mut h = BuzHasher::<N>::new();
        data.iter().map(|&b| h.push(b)).collect()
    }

    #[test]
    fn deterministic() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let first = buzhashes(data);
        let again = buzhashes(data);
        assert_eq!(first, again);
    }

    #[test]
    fn diverge_and_converge() {
        let s1 = b"rhababer-barbara-bar-barbaren-bier-bar-baerbel";
        let s2 = b"rhababer-barbara-cafe-clowns-bier-bar-baerbel";
        let h1 = buzhashes(s1);
        let h2 = buzhashes(s2);
        assert_eq!(h1[..17], h2[..17]);
        for i in 17..35 {
            assert_ne!(h1[i], h2[i], "i={i}");
            assert_ne!(h1[i + 1], h2[i], "i={i}");
        }
        assert_eq!(h1[36..], h2[35..]);
    }

    #[test]
    fn adjacent_windows_differ() {
        let hashes = buzhashes(b"abcdefghi");
        for i in 1..hashes.len() {
            assert_ne!(hashes[i - 1], hashes[i]);
        }
    }
}
