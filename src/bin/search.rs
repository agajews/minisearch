use std::convert::TryInto;
use ::minisearch::sparse::SparseU32Vec;
use std::fs;
use std::path::PathBuf;
use fasthash::metro::hash64;
use std::error::Error;

struct Index {
    terms: Vec<(u64, SparseU32Vec)>,
    urls: Vec<String>,
    n_terms: Vec<u32>,
}

impl Index {
    fn load(path: PathBuf) -> Result<Index, Box<dyn Error>> {
        let metadata = Self::deserialize_metadata(&fs::read(path.join("metadata.bytes"))?)?;
        println!("got metadata");
        let terms = Self::deserialize_terms(metadata, &fs::read(path.join("terms.bytes"))?)?;
        println!("got terms");
        let urls = Self::deserialize_urls(&fs::read(path.join("urls.bytes"))?)?;
        println!("got urls");
        let n_terms = Self::deserialize_n_terms(&fs::read(path.join("n_terms.bytes"))?)?;
        Ok(Index {
            terms,
            urls,
            n_terms,
        })
    }

    fn deserialize_metadata(encoded: &[u8]) -> Result<Vec<(u64, (u32, u32))>, Box<dyn Error>> {
        let n_terms = encoded.len() / 16;
        let mut metadata = Vec::with_capacity(n_terms);
        let mut i = 0;
        for _ in 0..n_terms {
            let hash = u64::from_be_bytes(encoded[i..(i + 8)].try_into()?);
            i += 8;
            let start = u32::from_be_bytes(encoded[i..(i + 4)].try_into()?);
            i += 4;
            let end = u32::from_be_bytes(encoded[i..(i + 4)].try_into()?);
            i += 4;
            metadata.push((hash, (start, end)));
        }
        Ok(metadata)
    }

    fn deserialize_terms(metadata: Vec<(u64, (u32, u32))>, encoded: &[u8]) -> Result<Vec<(u64, SparseU32Vec)>, Box<dyn Error>> {
        let mut terms = Vec::with_capacity(metadata.len());
        for (hash, (start, end)) in metadata {
            let vec = SparseU32Vec::deserialize(&encoded[start as usize..end as usize])?;
            terms.push((hash, vec));
        }
        Ok(terms)
    }

    fn deserialize_urls(encoded: &[u8]) -> Result<Vec<String>, Box<dyn Error>> {
        let n_urls = u32::from_be_bytes(encoded[0..4].try_into()?) as usize;
        let mut headers = Vec::with_capacity(n_urls);
        let mut i = 4;
        for _ in 0..n_urls {
            let offset = u32::from_be_bytes(encoded[i..(i + 4)].try_into()?);
            i += 4;
            let len = u32::from_be_bytes(encoded[i..(i + 4)].try_into()?);
            i += 4;
            headers.push((offset, len));
        }
        let header_len = 4 + 8 * n_urls;

        let mut urls = Vec::with_capacity(n_urls);
        for (offset, len) in headers {
            let start = header_len + offset as usize;
            let url = String::from_utf8(encoded[start..(start + len as usize)].to_vec())?;
            urls.push(url);
        }
        Ok(urls)
    }

    fn deserialize_n_terms(encoded: &[u8]) -> Result<Vec<u32>, Box<dyn Error>> {
        let n = encoded.len() / 4;
        let mut n_terms = Vec::with_capacity(n);
        let mut i = 0;
        for _ in 0..n {
            let count = u32::from_be_bytes(encoded[i..(i + 4)].try_into()?);
            i += 4;
            n_terms.push(count);
        }
        Ok(n_terms)
    }

    fn get_count(&self, term: &str) -> Option<Vec<u32>> {
        let i = self.terms.binary_search_by_key(&hash64(term), |(a, _)| *a).ok()?;
        let (_, counts) = &self.terms[i];
        let counts = counts.make_dense(self.urls.len());
        Some(counts)
    }

    fn search(&self, terms: Vec<String>) -> Option<Vec<(String, f32)>> {
        let counts = terms.into_iter().map(|term| self.get_count(&term))
            .collect::<Option<Vec<_>>>()?;
        let dfs = counts.iter().map(|counts| counts.iter().filter(|c| **c > 0).count())
            .map(|c| (c as f32).log2())
            .collect::<Vec<_>>();
        let mut scores = Vec::with_capacity(counts.len());
        for i in 0..(self.urls.len()) {
            let mut score = 0.0;
            for j in 0..(counts.len()) {
                score += counts[j][i] as f32 / self.n_terms[i] as f32 / dfs[j];
            }
            if score > 0.0 {
                scores.push((self.urls[i].clone(), score));
            }
        }
        // reverse sort
        scores.sort_by(|(_, score_a), (_, score_b)| score_b.partial_cmp(score_a).unwrap());
        Some(scores)
    }
}

fn main() {
    let index = Index::load("/tmp/pg".into()).unwrap();
    let matches = index.search(vec!["fundraising".to_string(), "growth".to_string()]).unwrap();
    for (url, score) in matches {
        println!("{}: {}", url, score);
    }
}
