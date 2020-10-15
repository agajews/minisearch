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
use std::path::{Path, PathBuf};
use std::fs::{File, create_dir_all};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use ::minisearch::sparse::SparseU32Vec;

struct Client {
    client: reqwest::Client,
}

impl Client {
    fn new() -> Client {
        let client = reqwest::Client::builder()
            .user_agent("Minisearch/0.2")
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

    fn extract_links(&self, base_url: &Url, document: &str) -> Vec<Url> {
        let parent_host = base_url.host_str();
        let links = self.link_re.find_iter(document)
            .map(|m| m.as_str())
            .map(|s| &s[6..s.len() - 1])
            .filter_map(|href| base_url.join(href).ok())
            .filter(|url| url.host_str() == parent_host)
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

struct Index {
    terms: BTreeMap<String, SparseU32Vec>,
    n_terms: Vec<u32>,
    urls: Vec<String>,
    n: u32,
    path: PathBuf,
}

impl Index {
    fn new(path: PathBuf) -> Index {
        create_dir_all(&path).unwrap();
        Index {
            path,
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

    fn dump(&self) {
        let mut encoded_terms = Vec::new();
        let mut metadata = BTreeMap::new();
        for (term, vec) in &self.terms {
            let start = encoded_terms.len() as u32;
            encoded_terms.extend_from_slice(&vec.serialize());
            let end = encoded_terms.len() as u32;
            metadata.insert(hash64(term), (start, end));
        }

        // TODO: better serialization abstraction
        Self::sync_write(self.path.join("terms.bytes"), &encoded_terms);
        Self::sync_write(self.path.join("metadata.bytes"), &Self::serialize_metadata(metadata));
        Self::sync_write(self.path.join("urls.bytes"), &Self::serialize_urls(&self.urls));
        Self::sync_write(self.path.join("n_terms.bytes"), &Self::serialize_n_terms(&self.n_terms));
    }

    fn serialize_metadata(metadata: BTreeMap<u64, (u32, u32)>) -> Vec<u8> {
        let mut encoded = Vec::new();
        for (hash, (start, end)) in metadata {
            encoded.extend_from_slice(&hash.to_be_bytes());
            encoded.extend_from_slice(&start.to_be_bytes());
            encoded.extend_from_slice(&end.to_be_bytes());
        }
        encoded
    }

    fn serialize_urls(urls: &Vec<String>) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&(urls.len() as u32).to_be_bytes());
        let mut i: u32 = 0;
        for url in urls {
            encoded.extend_from_slice(&i.to_be_bytes());
            encoded.extend_from_slice(&(url.len() as u32).to_be_bytes());
            i += url.len() as u32;
        }
        for url in urls {
            encoded.extend_from_slice(url.as_bytes());
        }
        encoded
    }

    fn serialize_n_terms(n_terms: &Vec<u32>) -> Vec<u8> {
        let mut encoded = Vec::new();
        for k in n_terms {
            encoded.extend_from_slice(&k.to_be_bytes());
        }
        encoded
    }

    fn sync_write<P: AsRef<Path>>(path: P, bytes: &[u8]) {
        let mut file = File::create(path).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }
}

// async fn handle_url(
//     hosts: &Vec<String>,
//     client: &Client,
//     seen: &cbloom::Filter,
//     link_extractor: &LinkExtractor,
//     term_extractor: &TermExtractor,
//     index: &mut Index,
//     url: Url,
//     urls: &mut VecDeque<Url>,
// ) -> Option<()> {
//     let text = match client.fetch(url.clone()).await {
//         Ok(text) => text,
//         Err(err) => {
//             println!("failed to crawl {:?}: {:?}", url, err);
//             return None;
//         },
//     };
//     let links = link_extractor.extract_links(hosts, &url, &text);
//     for link in links {
//         let hash = hash64(link.as_str());
//         if !seen.maybe_contains(hash) {
//             seen.insert(hash);
//             urls.push_back(link);
//         }
//     }
//     let digest = term_extractor.digest(url.into_string(), &text);
//     index.add(digest);
//     Some(())
// }

// #[tokio::main]
// async fn main() {
//     let mut urls = VecDeque::new();
//     let seen = cbloom::Filter::new(1_000_000, 100_000);
//     let sites = vec![
//         "http://paulgraham.com",
//         "http://blog.samaltman.com",
//         "http://www.catb.org",
//         "http://paulbuchheit.blogspot.com",
//         "https://www.joelonsoftware.com",
//         "https://blog.pmarca.com",
//         "https://www.scottaaronson.com",
//         "https://slatestarcodex.com",
//         "https://www.brainpickings.org",
//         "https://patrickcollison.com",
//         "https://www.gwern.net",
//         "https://marginalrevolution.com",
//         "http://lukemuehlhauser.com",
//         "https://lemire.me/blog/",
//         "https://guzey.com",
//         "https://nintil.com",
//         "https://jakeseliger.com",
//         "http://michaelnielsen.org",
//         "https://vitalik.ca",
//         "https://lacker.io",
//     ];
//     let mut hosts = Vec::new();
//     for site in sites {
//         let url = Url::parse(site).unwrap();
//         let host = String::from(url.host_str().unwrap());
//         hosts.push(host);
//         seen.insert(hash64(url.as_str()));
//         urls.push_back(url);
//     }
//     let client = Client::new();
//     let link_extractor = LinkExtractor::new();
//     let term_extractor = TermExtractor::new();
//     let mut index = Index::new("/tmp/alexsearch/".into());
//     loop {
//         let url = match urls.pop_front() {
//             Some(url) => url,
//             None => break,
//         };
//         println!("crawling {}", url.as_str());
//         handle_url(
//             &hosts,
//             &client,
//             &seen,
//             &link_extractor,
//             &term_extractor,
//             &mut index,
//             url,
//             &mut urls,
//         ).await;
//     }
//     index.dump()
// }

async fn crawler_thread(
    idx: usize,
    mut work_receiver: Receiver<Url>,
    mut index_sender: Sender<Digest>,
    mut done_sender: Sender<(usize, Vec<Url>)>,
    seen: Arc<cbloom::Filter>,
) {
    let link_extractor = LinkExtractor::new();
    let term_extractor = TermExtractor::new();
    let client = Client::new();
    while let Some(url) = work_receiver.recv().await {
        println!("crawling {}", url.as_str());

        let text = match client.fetch(url.clone()).await {
            Ok(text) => text,
            Err(err) => {
                println!("failed to crawl {:?}: {:?}", url, err);
                continue;
            },
        };
        let links = link_extractor.extract_links(&url, &text);
        let mut found = Vec::new();
        {
            for link in links {
                let hash = hash64(link.as_str());
                if !seen.maybe_contains(hash) {
                    seen.insert(hash);
                    found.push(link);
                }
            }
        }
        let digest = term_extractor.digest(url.into_string(), &text);
        if let Err(_) = index_sender.send(digest).await {
            panic!("index channel closed");
        };
        if let Err(_) = done_sender.send((idx, found)).await {
            panic!("done channel closed");
        };
    }
}

async fn host_thread(
    host: Url,
    index_sender: Sender<Digest>,
    seen: Arc<cbloom::Filter>,
    n_finished: Arc<AtomicUsize>,
) {
    let mut queue = VecDeque::new();
    let mut waiting = Vec::new();
    let (done_sender, mut done_receiver) = channel(4096);
    let mut work_senders = Vec::new();
    for idx in 0..THREADS_PER_HOST {
        let index_sender = index_sender.clone();
        let done_sender = done_sender.clone();
        let seen = seen.clone();
        let (work_sender, work_receiver) = channel(4096);
        work_senders.push(work_sender);
        tokio::spawn(async move {
            crawler_thread(idx, work_receiver, index_sender, done_sender, seen).await
        });
    }

    work_senders[0].send(host).await.unwrap();
    for i in 1..THREADS_PER_HOST {
        waiting.push(i);
    }

    while let Some((idx, found)) = done_receiver.recv().await {
        for url in found {
            queue.push_back(url);
        }
        waiting.push(idx);
        if queue.is_empty() && waiting.len() == THREADS_PER_HOST {
            break;
        }
        while let Some(idx) = waiting.pop() {
            match queue.pop_front() {
                Some(url) => work_senders[idx].send(url).await.unwrap(),
                None => {
                    waiting.push(idx);
                    break;
                },
            }
        }
    }

    n_finished.fetch_add(1, Ordering::Relaxed);
}

const THREADS_PER_HOST: usize = 10;

#[tokio::main]
async fn main() {
    let hosts = vec![
        "http://paulgraham.com",
        "http://blog.samaltman.com",
        "http://www.catb.org",
        "http://paulbuchheit.blogspot.com",
        "https://www.joelonsoftware.com",
        "https://blog.pmarca.com",
        "https://www.scottaaronson.com",
        "https://slatestarcodex.com",
        "https://www.brainpickings.org",
        "https://patrickcollison.com",
        "https://www.gwern.net",
        "https://marginalrevolution.com",
        "http://lukemuehlhauser.com",
        "https://lemire.me/blog/",
        "https://guzey.com",
        "https://nintil.com",
        "https://jakeseliger.com",
        "http://michaelnielsen.org",
        "https://vitalik.ca",
        "https://lacker.io",
        "https://blog.zkga.me",
        "http://planetbanatt.net",
    ];

    let (index_sender, mut index_receiver) = channel(4096);
    let n_finished = Arc::new(AtomicUsize::new(0));
    let mut index = Index::new("./alexsearch/".into());
    let seen = Arc::new(cbloom::Filter::new(1_000_000, 100_000));
    for host in hosts.iter() {
        let index_sender = index_sender.clone();
        let seen = seen.clone();
        let n_finished = n_finished.clone();
        let host = Url::parse(host).unwrap();
        tokio::spawn(async move {
            host_thread(host, index_sender, seen, n_finished).await
        });
    }
    while let Some(digest) = index_receiver.recv().await {
        index.add(digest);
        let n_finished = n_finished.load(Ordering::Relaxed);
        println!("n finished: {}", n_finished);
        if n_finished == hosts.len() {
            break;
        }
    }
    println!("dumping index");
    index.dump();
    println!("done!");
}
