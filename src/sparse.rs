use std::convert::TryInto;
use std::error::Error;

#[derive(Debug)]
pub struct SparseU32Vec {
    data: Vec<u32>,
    indices: Vec<u32>,
}

impl SparseU32Vec {
    pub fn new() -> SparseU32Vec {
        SparseU32Vec {
            data: Vec::new(),
            indices: Vec::new(),
        }
    }

    pub fn add(&mut self, i: u32, x: u32) {
        self.data.push(x);
        self.indices.push(i);
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.extend_from_slice(&(self.data.len() as u32).to_be_bytes());
        for x in &self.data {
            serialized.extend_from_slice(&x.to_be_bytes());
        }
        for i in &self.indices {
            serialized.extend_from_slice(&i.to_be_bytes());
        }
        serialized
    }

    pub fn deserialize(serialized: &[u8]) -> Result<SparseU32Vec, Box<dyn Error>> {
        let len = u32::from_be_bytes(serialized[0..4].try_into()?) as usize;
        let mut data = Vec::new();
        let mut indices = Vec::new();
        for i in 0..len {
            let k = 4 + i * 4;
            let x = u32::from_be_bytes(serialized[k..(k + 4)].try_into()?);

            let k = 4 + len * 4 + i * 4;
            let idx = u32::from_be_bytes(serialized[k..(k + 4)].try_into()?);

            data.push(x);
            indices.push(idx);
        }

        Ok(SparseU32Vec { data, indices })
    }

    pub fn make_dense(&self, len: usize) -> Vec<u32> {
        let mut dense = vec![0; len];
        for (i, x) in self.indices.iter().zip(&self.data) {
            dense[*i as usize] = *x;
        }
        dense
    }
}

