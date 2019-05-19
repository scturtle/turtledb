use integer_encoding::{FixedIntWriter, VarIntWriter};

pub const BLOCK_SIZE: usize = 4 * 1024;
pub const BLOCK_RESTART_INTERVAL: usize = 16;

struct BlockBuilder {
    buffer: Vec<u8>,
    count: usize,
    restarts: Vec<usize>,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            buffer: vec![],
            count: 0,
            restarts: vec![],
            last_key: vec![],
        }
    }
    pub fn add(&mut self, key: &[u8], val: &[u8]) {
        debug_assert!(self.last_key.is_empty() || self.last_key.as_slice() <= key);
        // count the length of shared prefix between last_key and key
        let mut shared_cnt = 0;
        if self.count % BLOCK_RESTART_INTERVAL == 0 {
            self.restarts.push(self.buffer.len());
            self.last_key.clear();
        } else {
            if !self.last_key.is_empty() {
                let min_len = self.last_key.len().min(key.len());
                while shared_cnt < min_len && self.last_key[shared_cnt] == key[shared_cnt] {
                    shared_cnt += 1;
                }
            }
        }
        // shared_key_cnt, non_shared_key_cnt, val_cnt, non_shared key, val
        self.buffer.write_varint(shared_cnt).unwrap();
        self.buffer.write_varint(key.len() - shared_cnt).unwrap();
        self.buffer.write_varint(val.len()).unwrap();
        self.buffer.extend_from_slice(&key[shared_cnt..]);
        self.buffer.extend_from_slice(val);
        // update
        self.last_key = key.to_vec();
        self.count += 1;
    }
    pub fn done(&mut self) -> Vec<u8> {
        // restarts and count
        for i in &self.restarts {
            self.buffer.write_fixedint(*i as u32).unwrap();
        }
        self.buffer
            .write_fixedint(self.restarts.len() as u32)
            .unwrap();

        // compress
        let mut encoder = snap::Encoder::new();
        self.buffer = encoder
            .compress_vec(&self.buffer)
            .expect("snappy compression");
        self.buffer.push(1u8); // 1 for snappy

        // crc
        use crc::crc32::{Digest, Hasher32, CASTAGNOLI};
        let mut digest = Digest::new(CASTAGNOLI);
        digest.write(&self.buffer);
        // skip mask step
        self.buffer.write_fixedint(digest.sum32()).unwrap();

        // reset
        let mut result = vec![];
        std::mem::swap(&mut result, &mut self.buffer);
        self.count = 0;
        self.restarts.clear();
        self.last_key.clear();
        result
    }
}
