use crate::utils::hash;
use std::borrow::Borrow;

const BITS_PER_KEY: u8 = 10;
const K: u8 = 6; // BITS_PER_KEY * ln(2)

pub struct BloomFilter {
    k: u8,
    array: Vec<u8>,
}

impl BloomFilter {
    pub fn new<T: Borrow<[u8]>>(keys: &[T]) -> Self {
        let k = K;
        let n = keys.len();
        let bits = 64.max(BITS_PER_KEY as usize * n);
        let bytes = (bits + 7) / 8;
        let bits = bytes * 8;
        let mut bf = Self {
            k,
            array: std::iter::repeat(0).take(bytes).collect(),
        };
        for key in keys {
            let mut h = hash(key.borrow());
            let delta = h.rotate_right(17);
            for _ in 0..k {
                let bitpos = (h as usize) % bits;
                bf.array[bitpos / 8] |= 1 << (bitpos % 8);
                h = h.wrapping_add(delta);
            }
        }
        bf
    }
    pub fn key_may_match(&self, key: &[u8]) -> bool {
        let bits = self.array.len() * 8;
        let mut h = hash(key);
        let delta = h.rotate_right(17);
        for _ in 0..self.k {
            let bitpos = (h as usize) % bits;
            if self.array[bitpos / 8] & (1 << (bitpos % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloomfilter() {
        let mut keys = vec![];
        let limit: u32 = 100000;
        for i in (0..limit).step_by(2) {
            keys.push(i.to_string().into_bytes());
        }
        let bf = BloomFilter::new(&keys);
        let mut fp = 0.0;
        for i in 0..limit {
            let found = bf.key_may_match(&i.to_string().into_bytes());
            if i % 2 == 0 {
                assert_eq!(found, i % 2 == 0);
            } else {
                fp += found as u8 as f32;
            }
        }
        let fp_ratio = fp / (limit as f32);
        assert!(fp_ratio < 0.008);
    }
}
