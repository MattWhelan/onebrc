use std::collections::{BTreeMap, HashMap};
use std::env::args;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::thread;

#[derive(Debug, Clone)]
struct Record {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,
}

impl Default for Record {
    fn default() -> Self {
        Record {
            min: f32::MAX,
            max: f32::MIN,
            sum: 0.0,
            count: 0,
        }
    }
}

impl Record {
    fn add(&mut self, v: f32) {
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        self.sum += v;
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    fn mean(&self) -> f32 {
        self.sum / self.count as f32
    }
}

type Table = HashMap<Vec<u8>, Record>;

fn main() -> Result<(), Box<dyn Error>> {
    if let [_, filename, ..] = &args().collect::<Vec<_>>()[..] {
        let infile = File::open(filename)?;

        let mut buf = BufReader::with_capacity(2 * 1024 * 1024, infile);

        let mut processor = Processor::new();
        let result = processor.process(&mut buf)?;
        report(&result)
    } else {
        println!("Usage: onebrc <filename>");
        Ok(())
    }
}

struct Processor {}

impl Processor {
    fn new() -> Self {
        Self {}
    }

    fn process(&mut self, buf: &mut BufReader<File>) -> Result<Table, Box<dyn Error>> {
        let mut prefix = Vec::with_capacity(1024);

        let (chunk_tx, chunk_rx) = crossbeam::channel::bounded::<Vec<u8>>(20);

        let mut workers = Vec::new();
        for _ in 0..12 {
            let rx = chunk_rx.clone();
            workers.push(thread::spawn(move || {
                let mut result = Table::new();
                rx.iter()
                    .for_each(|chunk| {
                        chunk.split(|b| *b == b'\n')
                            .map(|bs| Self::process_line(bs))
                            .for_each(|(k, v)| {
                                let e = result.entry(k).or_default();
                                e.add(v);
                            })
                    });

                result
            }));
        }

        loop {
            let bytes = buf.fill_buf().unwrap();
            if bytes.is_empty() {
                break;
            }

            if let Some(pos) = bytes.iter().rposition(|b| *b == b'\n') {
                let mut chunk = Vec::with_capacity(prefix.len() + pos);
                chunk.extend_from_slice(&prefix);
                chunk.extend_from_slice(&bytes[0..pos]);

                chunk_tx.send(chunk)?;

                prefix.clear();
                prefix.extend_from_slice(&bytes[pos + 1..]);
            } else {
                dbg!("Big line!");
                prefix.extend_from_slice(bytes);
            }
            let length = bytes.len();
            buf.consume(length);
        }
        drop(chunk_tx);

        let table = workers.into_iter().map(|w| w.join().unwrap())
            .reduce(|mut l, r| {
                Self::merge_maps(&mut l, r);
                l
            });

        Ok(table.unwrap())
    }

    fn process_chunk(chunk: Vec<u8>) -> Table {
        chunk.split(|b| *b == b'\n')
            .map(|bs| Self::process_line(bs))
            .fold(HashMap::new(), |mut acc, (k, v)| {
                let e: &mut Record = acc.entry(k).or_default();
                e.add(v);
                acc
            })
    }

    fn merge_maps(l: &mut Table, r: Table) {
        r.into_iter().for_each(|(k, v)| {
            let e = l.entry(k).or_default();
            e.merge(&v);
        });
    }

    fn process_line(bs: &[u8]) -> (Vec<u8>, f32) {
        if let Some(pos) = bs.iter().rposition(|b| *b == b';') {
            let name = &bs[0..pos];
            let num = &bs[pos + 1..];

            let v = parse_decimal(num);
            // let float_str = std::str::from_utf8(num).expect("bad utf8");
            // let v = f32::from_str(float_str).expect("expected float str");

            (name.to_owned(), v)
        } else {
            panic!("missing semicolon: {}", String::from_utf8_lossy(bs))
        }
    }
}

fn parse_decimal(bs: &[u8]) -> f32 {
    let mut n = 0;
    let mut signum = 1;
    let mut dot = bs.len() - 1;
    for (i, &b) in bs.iter().enumerate() {
        match b {
            b'-' => {
                signum *= -1;
            }
            b'0'..=b'9' => {
                let v = b - b'0';
                n = n * 10 + signum * (v as i32)
            }
            b'.' => {
                dot = i;
            }
            _ => panic!("bad decimal character {b}")
        }
    }

    let n = n as f32;
    match (bs.len() - 1 - dot) as i32 {
        0 => n,
        1 => n / 10.0,
        2 => n / 100.0,
        3 => n / 1000.0,
        _ => {
            let d = (10.0f32).powi((bs.len() - 1 - dot) as i32);
            n / d
        }
    }
}

fn count_buffer(buf: &mut BufReader<File>) -> Result<(), Box<dyn Error>> {
    let mut count = 0;
    loop {
        let bytes = buf.fill_buf()?;
        let size = bytes.len();
        if size > 0 {
            count += size;
            buf.consume(size);
        } else {
            break;
        }
    }

    println!("Count {count}");
    Ok(())
}

fn report(table: &Table) -> Result<(), Box<dyn Error>> {
    let mut stdout = std::io::stdout().lock();
    write!(stdout, "{{")?;

    let table: BTreeMap<String, &Record> = table.iter().map(|(k, v)| {
        let city_str = String::from_utf8_lossy(k);
        (city_str.to_string(), v)
    })
        .collect();
    for (city, record) in table.into_iter() {
        write!(stdout, "{city}={:.1}/{:.1}/{:.1}, ", record.min, record.mean(), record.max)?;
    }
    writeln!(stdout, "}}")?;
    Ok(())
}
