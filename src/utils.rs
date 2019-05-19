use integer_encoding::FixedInt;
use std::borrow::Borrow;

const BLOCK_SIZE: usize = 4 * 1024;

pub fn hash<T: Borrow<[u8]>>(data: T) -> u32 {
    let data = data.borrow();
    const SEED: u32 = 0xbc9f1d34;
    const M: u32 = 0xc6a4a793;
    const R: u32 = 24;
    let n = data.len();
    let mut h = SEED ^ ((n as u32).wrapping_mul(M));

    let mut i: usize = 0;
    while i + 4 <= n {
        let w = u32::decode_fixed(&data[i..i + 4]);
        i += 4;
        h = h.wrapping_add(w);
        h = h.wrapping_mul(M);
        h ^= h >> 16;
    }
    // remain
    if n - i == 3 {
        h = h.wrapping_add((data[i + 2] as u32) << 16);
    }
    if n - i >= 2 {
        h = h.wrapping_add((data[i + 1] as u32) << 8);
    }
    if n - i >= 1 {
        h = h.wrapping_add(data[i] as u32);
        h = h.wrapping_mul(M);
        h ^= h >> R;
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash() {
        let mut data = [0u8; 256];
        for i in 0..256 {
            data[i as usize] = i as u8;
        }
        assert_eq!(hash(&data[..253][..]), 4252827879u32);
        assert_eq!(hash(&data[..254][..]), 1201313498u32);
        assert_eq!(hash(&data[..255][..]), 4195008990u32);
        assert_eq!(hash(&data[..]), 2717871074u32);
    }
}
