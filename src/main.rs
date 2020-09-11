use reqwest;
use tokio;
use url::Url;
use std::collections::{VecDeque, BTreeMap};
use reqwest::redirect::Policy;
use std::time::Duration;
use std::error::Error;
use tokio::stream::StreamExt;
use regex::Regex;
use http::Uri;
use cbloom;
use fasthash::metro::hash64;
use std::convert::TryInto;

struct Client {
    client: reqwest::Client,
}

impl Client {
    fn new() -> Client {
        let client = reqwest::Client::builder()
            .user_agent("Minisearch/0.1")
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .redirect(Policy::limited(100))
            .timeout(Duration::from_secs(60))
            .build().unwrap();
        Client {
            client
        }
    }

    async fn fetch(&self, url: Url) -> Result<String, Box<dyn Error>> {
        let res = self.client.get(url).send().await?;
        let mut stream = res.bytes_stream();
        let mut total_len: usize = 0;
        let mut data = Vec::new();
        while let Some(Ok(chunk)) = stream.next().await {
            total_len += chunk.len();
            if total_len > 100_000 {
                break;
            }
            data.extend_from_slice(&chunk);
        }
        Ok(String::from_utf8_lossy(&data).to_string())
    }
}

async fn handle_url(
    host: &str,
    client: &Client,
    seen: &cbloom::Filter,
    link_extractor: &LinkExtractor,
    term_extractor: &TermExtractor,
    index: &mut Index,
    url: Url,
    urls: &mut VecDeque<Url>,
) -> Option<()> {
    let text = match client.fetch(url.clone()).await {
        Ok(text) => text,
        Err(err) => {
            println!("failed to crawl {:?}: {:?}", url, err);
            return None;
        },
    };
    let links = link_extractor.extract_links(host, &url, &text);
    for link in links {
        let hash = hash64(link.as_str());
        if !seen.maybe_contains(hash) {
            seen.insert(hash);
            urls.push_back(link);
        }
    }
    let digest = term_extractor.digest(url.into_string(), &text);
    index.add(digest);
    Some(())
}

struct Digest {
    terms: BTreeMap<String, u32>,
    n_terms: u32,
    url: String,
}

struct TermExtractor {
    body_re: Regex,
    tag_text_re: Regex,
    term_re: Regex,
}

impl TermExtractor {
    fn new() -> TermExtractor {
        TermExtractor {
            body_re: Regex::new(r"(?s)<(body|/script|/style)([^<>]*)>.*?(</body>|<script|<style)").unwrap(),
            tag_text_re: Regex::new(r">([^<>]+)").unwrap(),
            term_re: Regex::new(r"[a-zA-Z]+").unwrap(),
        }
    }

    fn digest(&self, url: String, document: &str) -> Digest {
        let mut terms = BTreeMap::<String, u32>::new();
        let mut n_terms = 0;
        for section in self.body_re.find_iter(document) {
            for tag_text in self.tag_text_re.captures_iter(section.as_str()) {
                for term in self.term_re.find_iter(&tag_text[1]) {
                    let term = term.as_str().to_lowercase();
                    // println!("{}", term);
                    *terms.entry(term).or_insert(0) += 1;
                    n_terms += 1;
                }
            }
        }

        Digest { terms, n_terms, url }
    }
}

struct LinkExtractor {
    link_re: Regex,
}

impl LinkExtractor {
    fn new() -> LinkExtractor {
        LinkExtractor {
            link_re: Regex::new("href=['\"][^'\"]+['\"]").unwrap(),
        }
    }

    fn clearly_not_html(url: &str) -> bool {
        url.ends_with(".css") ||
            url.ends_with(".js") ||
            url.ends_with(".mp3") ||
            url.ends_with(".mp4") ||
            url.ends_with(".m4v") ||
            url.ends_with(".mov") ||
            url.ends_with(".dmg") ||
            url.ends_with(".pt") ||
            url.ends_with(".vdi") ||
            url.ends_with(".ova") ||
            url.ends_with(".m2ts") ||
            url.ends_with(".rmvb") ||
            url.ends_with(".npz") ||
            url.ends_with(".mat") ||
            url.ends_with(".data") ||
            url.ends_with(".xml") ||
            url.ends_with(".7z") ||
            url.ends_with(".gz") ||
            url.ends_with(".gztar") ||
            url.ends_with(".pdf") ||
            url.ends_with(".png") ||
            url.ends_with(".PNG") ||
            url.ends_with(".ico") ||
            url.ends_with(".ICO") ||
            url.ends_with(".jpg") ||
            url.ends_with(".JPG") ||
            url.ends_with(".gif") ||
            url.ends_with(".GIF") ||
            url.ends_with(".svg") ||
            url.ends_with(".SVG") ||
            url.ends_with(".json") ||
            !url.starts_with("http")
    }

    fn extract_links(&self, host: &str, base_url: &Url, document: &str) -> Vec<Url> {
        let links = self.link_re.find_iter(document)
            .map(|m| m.as_str())
            .map(|s| &s[6..s.len() - 1])
            .filter_map(|href| base_url.join(href).ok())
            .filter(|url| url.host_str() == Some(host))
            .collect::<Vec<_>>();
        // if links.iter().filter_map(Self::looks_like_a_trap).any(|x| x) {
        //     return;
        // }
        let links = links.into_iter()
            .map(|mut url| {
                url.set_fragment(None);
                url.set_query(None);
                url.into_string()
            })
            .filter(|url| !Self::clearly_not_html(url))
            .filter(|url| url.len() <= 300)
            .filter(|url| url.parse::<Uri>().is_ok())
            .filter_map(|url| url.parse::<Url>().ok())
            .collect::<Vec<_>>();

        links
    }
}

struct SparseU32Vec {
    data: Vec<u32>,
    indices: Vec<u32>,
}

impl SparseU32Vec {
    fn new() -> SparseU32Vec {
        SparseU32Vec {
            data: Vec::new(),
            indices: Vec::new(),
        }
    }

    fn add(&mut self, i: u32, x: u32) {
        self.data.push(x);
        self.indices.push(i);
    }

    fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.extend_from_slice(&self.data.len().to_be_bytes());
        for x in &self.data {
            serialized.extend_from_slice(&x.to_be_bytes());
        }
        for i in &self.indices {
            serialized.extend_from_slice(&i.to_be_bytes());
        }
        serialized
    }

    fn deserialize(serialized: &[u8]) -> Option<SparseU32Vec> {
        let len = u32::from_be_bytes(serialized[0..4].try_into().ok()?) as usize;
        let mut data = Vec::new();
        let mut indices = Vec::new();
        for i in 0..len {
            let k = 4 + i * 4;
            let x = u32::from_be_bytes(serialized[k..(k + 4)].try_into().ok()?);

            let k = 4 + len * 4 + i * 4;
            let idx = u32::from_be_bytes(serialized[k..(k + 4)].try_into().ok()?);

            data.push(x);
            indices.push(idx);
        }

        Some(SparseU32Vec { data, indices })
    }

    fn make_dense(&self, len: usize) -> Vec<u32> {
        let mut dense = vec![0; len];
        for (i, x) in self.indices.iter().zip(&self.data) {
            dense[*i as usize] = *x;
        }
        dense
    }
}

struct Index {
    terms: BTreeMap<String, SparseU32Vec>,
    n_terms: Vec<u32>,
    urls: Vec<String>,
    n: u32,
}

impl Index {
    fn new() -> Index {
        Index {
            terms: BTreeMap::new(),
            n_terms: Vec::new(),
            urls: Vec::new(),
            n: 0,
        }
    }

    fn add(&mut self, digest: Digest) {
        for (term, count) in digest.terms {
            let rle = self.terms.entry(term).or_insert_with(|| SparseU32Vec::new());
            rle.add(self.n, count);
        }
        self.n_terms.push(digest.n_terms);
        self.urls.push(digest.url);
        self.n += 1;
    }
}

#[tokio::main]
async fn main() {
    let mut urls = VecDeque::new();
    let seen = cbloom::Filter::new(1_000_000, 100_000);
    let pg = Url::parse("http://paulgraham.com").unwrap();
    let host = String::from(pg.host_str().unwrap());
    urls.push_back(pg);
    let client = Client::new();
    let link_extractor = LinkExtractor::new();
    let term_extractor = TermExtractor::new();
    let mut index = Index::new();
    loop {
        let url = match urls.pop_front() {
            Some(url) => url,
            None => break,
        };
        println!("crawling {}", url.as_str());
        handle_url(
            &host,
            &client,
            &seen,
            &link_extractor,
            &term_extractor,
            &mut index,
            url,
            &mut urls,
        ).await;
    }
}
