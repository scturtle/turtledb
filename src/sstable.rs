use crate::bloomfilter::BloomFilter;
use crc::crc32::{Digest, Hasher32, CASTAGNOLI};
use integer_encoding::{FixedInt, FixedIntWriter, VarIntWriter};

pub const BLOCK_SIZE: usize = 4 * 1024;
pub const BLOCK_RESTART_INTERVAL: usize = 16;

struct BlockBuilder {
    buffer: Vec<u8>,
    count: usize,
    restart_interval: usize,
    restarts: Vec<usize>,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        Self {
            buffer: vec![],
            count: 0,
            restart_interval,
            restarts: vec![],
            last_key: vec![],
        }
    }
    pub fn add(&mut self, key: &[u8], val: &[u8]) {
        debug_assert!(self.last_key.is_empty() || self.last_key.as_slice() <= key);
        // count the length of shared prefix between last_key and key
        let mut shared_cnt = 0;
        if self.count % self.restart_interval == 0 {
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
        let mut digest = Digest::new(CASTAGNOLI);
        digest.write(&self.buffer);
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

struct Block {
    data: Vec<u8>,
    restarts: Vec<usize>,
}

impl Block {
    pub fn load(raw: &[u8]) -> Option<Self> {
        if raw.len() < 4 {
            return None;
        }
        let raw_without_crc = &raw[..raw.len() - 4];
        let crc = u32::decode_fixed(&raw[raw.len() - 4..]);
        // check crc
        let mut digest = Digest::new(CASTAGNOLI);
        digest.write(raw_without_crc);
        if digest.sum32() != crc {
            return None;
        }
        // decompress
        let mut decoder = snap::Decoder::new();
        let mut data = decoder.decompress_vec(raw_without_crc).ok()?;
        if data.len() < 4 {
            return None;
        }
        // extract restarts
        let restart_cnt = u32::decode_fixed(&data[data.len() - 4..]) as usize;
        if data.len() < 4 + 4 * restart_cnt {
            return None;
        }
        let offset = data.len() - 4 - 4 * restart_cnt;
        let mut restarts = Vec::with_capacity(restart_cnt);
        for i in 0..restart_cnt {
            let buf = &data[offset + 4 * i..offset + 4 * (i + 1)];
            restarts.push(u32::decode_fixed(buf) as usize);
        }
        data.resize(offset, 0);
        Some(Self { data, restarts })
    }
}

struct FiltersBuilder {
    filters: Vec<BloomFilter>,
}

impl FiltersBuilder {
    pub fn new() -> Self {
        Self { filters: vec![] }
    }
    pub fn add_filter(&mut self, keys: &Vec<Vec<u8>>) {
        let mut filter = BloomFilter::new(keys.len());
        for key in keys {
            filter.add(key);
        }
        self.filters.push(filter);
    }
    pub fn done(&mut self) -> Vec<u8> {
        let mut buffer = vec![];
        let mut filters = vec![];
        std::mem::swap(&mut filters, &mut self.filters);
        // filters
        let mut filter_offsets = vec![];
        for f in filters {
            filter_offsets.push(buffer.len());
            let bytes = f.into_vec();
            buffer.write_fixedint(bytes.len() as u32).unwrap();
            buffer.extend(bytes);
        }
        // offsets
        let offset_of_offsets = buffer.len();
        for o in filter_offsets {
            buffer.write_fixedint(o as u32).unwrap();
        }
        buffer.write_fixedint(offset_of_offsets as u32).unwrap();
        // compress
        let mut encoder = snap::Encoder::new();
        buffer = encoder.compress_vec(&buffer).expect("snappy compression");
        // crc
        let mut digest = Digest::new(CASTAGNOLI);
        digest.write(&buffer);
        buffer.write_fixedint(digest.sum32()).unwrap();
        buffer
    }
}

struct TableBuilder {
    buffer: Vec<u8>,
    data_builder: BlockBuilder,
    index_builder: BlockBuilder,
    filters_builder: FiltersBuilder,
    curr_keys: Vec<Vec<u8>>,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            buffer: vec![],
            data_builder: BlockBuilder::new(BLOCK_RESTART_INTERVAL),
            index_builder: BlockBuilder::new(2),
            filters_builder: FiltersBuilder::new(),
            curr_keys: vec![],
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
        self.filters_builder.add_filter(&self.curr_keys);
        // write to buffer
        let offset = self.buffer.len();
        let bytes = self.data_builder.done();
        let length = bytes.len();
        self.buffer.extend(bytes);
        // record index
        let mut oh = vec![];
        oh.write_varint(offset).unwrap();
        oh.write_varint(length).unwrap();
        dbg!(&self.curr_keys);
        self.index_builder.add(self.curr_keys.last().unwrap(), &oh);
        // reset
        self.curr_keys.clear();
    }
    pub fn done(&mut self) -> Vec<u8> {
        // finish last block
        self.finish_curr_data_block();
        // write fitler block
        let offset_of_filters = self.buffer.len();
        let bytes = self.filters_builder.done();
        let length_of_filters = bytes.len();
        self.buffer.extend(bytes);
        // write index block
        let offset_of_index = self.buffer.len();
        let bytes = self.index_builder.done();
        let length_of_index = bytes.len();
        self.buffer.extend(bytes);
        // write footer
        self.buffer
            .write_fixedint(offset_of_filters as u32)
            .unwrap();
        self.buffer
            .write_fixedint(length_of_filters as u32)
            .unwrap();
        self.buffer.write_fixedint(offset_of_index as u32).unwrap();
        self.buffer.write_fixedint(length_of_index as u32).unwrap();
        self.buffer.extend("SCTURTLE".as_bytes().to_vec());
        // reset
        let mut result = vec![];
        std::mem::swap(&mut result, &mut self.buffer);
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
