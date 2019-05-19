use crate::utils::hash;

const BITS_PER_KEY: u8 = 10;
const K: u8 = 6; // BITS_PER_KEY * ln(2)

pub struct BloomFilter {
    k: u8,
    array: Vec<u8>,
}

impl BloomFilter {
    pub fn new(n_keys: usize) -> Self {
        let k = K;
        let bits = 64.max(BITS_PER_KEY as usize * n_keys);
        let bytes = (bits + 7) / 8;
        Self {
            k,
            array: vec![0u8; bytes],
        }
    }
    pub fn add(&mut self, key: &[u8]) {
        let bits = self.array.len() * 8;
        let mut h = hash(key);
        let delta = h.rotate_right(17);
        for _ in 0..self.k {
            let bitpos = (h as usize) % bits;
            self.array[bitpos / 8] |= 1 << (bitpos % 8);
            h = h.wrapping_add(delta);
        }
    }
    pub fn find(&self, key: &[u8]) -> bool {
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
        let limit = 100000;
        let mut bf = BloomFilter::new(limit / 2);
        for i in (0..limit).step_by(2) {
            bf.add(&i.to_string().into_bytes());
        }
        let mut fp = 0.0;
        for i in 0..limit {
            let found = bf.find(&i.to_string().into_bytes());
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
