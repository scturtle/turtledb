use crate::bloomfilter::BloomFilter;
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
    pub fn size(&self) -> usize {
        self.buffer.len() + 4 * self.restarts.len()
    }
}

struct TableBuilder {
    buffer: Vec<u8>,
    data_builder: BlockBuilder,
    index_builder: BlockBuilder,
    curr_keys: Vec<Vec<u8>>,
    filters: Vec<BloomFilter>,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            buffer: vec![],
            data_builder: BlockBuilder::new(),
            index_builder: BlockBuilder::new(),
            curr_keys: vec![],
            filters: vec![],
        }
    }
    pub fn add(&mut self, key: &[u8], val: &[u8]) {
        if self.data_builder.size() > BLOCK_SIZE {
            self.finish_curr_data_block();
        }
        self.data_builder.add(key, val);
        self.curr_keys.push(key.to_vec());
    }
    pub fn finish_curr_data_block(&mut self) {
        debug_assert!(self.curr_keys.len() > 0);
        // build filter
        let mut filter = BloomFilter::new(self.curr_keys.len());
        for key in &self.curr_keys {
            filter.add(key);
        }
        self.filters.push(filter);
        // write to buffer
        let offset = self.buffer.len();
        let bytes = self.data_builder.done();
        let length = bytes.len();
        self.buffer.extend(bytes);
        // record index
        let mut oh = vec![];
        oh.write_varint(offset).unwrap();
        oh.write_varint(length).unwrap();
        self.index_builder.add(self.curr_keys.last().unwrap(), &oh);
        // reset
        self.curr_keys.clear();
    }
    pub fn done(&mut self) -> Vec<u8> {
        // finish last block
        self.finish_curr_data_block();
        // write fitler block
        let mut filters = vec![];
        std::mem::swap(&mut filters, &mut self.filters);
        let mut filter_offsets = vec![];
        for f in filters {
            filter_offsets.push(self.buffer.len());
            let bytes = f.into_vec();
            self.buffer.write_fixedint(bytes.len() as u32).unwrap();
            self.buffer.extend(bytes);
        }
        let offset_of_filter_offsets = self.buffer.len();
        for o in filter_offsets {
            self.buffer.write_fixedint(o as u32).unwrap();
        }
        // write index block
        let offset_of_index = self.buffer.len();
        let bytes = self.index_builder.done();
        let length_of_index = bytes.len();
        self.buffer.extend(bytes);
        // write footer
        self.buffer
            .write_fixedint(offset_of_filter_offsets as u32)
            .unwrap();
        self.buffer.write_fixedint(offset_of_index as u32).unwrap();
        self.buffer.write_fixedint(length_of_index as u32).unwrap();
        self.buffer.extend(" SCT".as_bytes().to_vec());

        // reset
        let mut result = vec![];
        std::mem::swap(&mut result, &mut self.buffer);
        self.filters.clear();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_builder() -> std::io::Result<()> {
        let mut keys = vec![];
        let limit = 100;
        for i in 0..limit {
            keys.push(i.to_string().into_bytes());
        }
        keys.sort();
        let mut tb = TableBuilder::new();
        for i in 0..limit {
            tb.add(&keys[i], &keys[i]);
        }
        use std::io::Write;
        let mut file = std::fs::File::create("/tmp/test.sst")?;
        file.write_all(&mut tb.done())?;
        Ok(())
    }
}
