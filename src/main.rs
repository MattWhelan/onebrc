use std::collections::BTreeMap;
use std::env::args;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use rustc_hash::{FxBuildHasher, FxHashMap as HashMap};

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

impl From<f32> for Record {
    fn from(value: f32) -> Self {
        Record {
            min: value,
            max: value,
            sum: value,
            count: 1,
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

        let buf = BufReader::with_capacity(2 * 1024 * 1024, infile);

        let result = produce_table(buf);
        report(&result)
    } else {
        println!("Usage: onebrc <filename>");
        Ok(())
    }
}

fn insert_or_update(table: &mut Table, k: &[u8], v: f32) {
    if let Some(r) = table.get_mut(k) {
        r.add(v);
    } else {
        let r = Record::from(v);
        table.insert(Vec::from(k), r);
    }
}

fn produce_table(mut reader: BufReader<File>) -> Table {
    let mut table = Table::with_capacity_and_hasher(10_000, FxBuildHasher);

    let mut stash = Vec::with_capacity(100);

    while let Ok(mut buf) = reader.fill_buf() {
        if buf.is_empty() {
            break;
        }
        let mut it = buf.iter().enumerate();
        if let Some((sep, _)) = it.find(|(_, &b)| b == b';') {
            if let Some((end, _)) = it.find(|(_, &b)| b == b'\n') {
                let (name, rest) = buf.split_at(sep);
                let (val, _) = rest[1..].split_at(end - sep - 1);

                let v = parse_decimal(val);

                //dbg!(String::from_utf8_lossy(name), v);
                insert_or_update(&mut table, name, v);
                reader.consume(end+1);
            } else {
                // didn't get to the newline
                stash.extend_from_slice(buf);
                let consumed = buf.len();
                reader.consume(consumed);
                buf = reader.fill_buf().unwrap();
                let mut it = buf.iter().enumerate();
                if let Some((end, _)) = it.find(|(_, &b)| b == b'\n') {
                    stash.extend_from_slice(&buf[..end]);
                    let (name, rest) = stash.split_at(sep);
                    let val = &rest[1..];
                    let v = parse_decimal(val);

                    // dbg!(String::from_utf8_lossy(name), v);
                    insert_or_update(&mut table, name, v);
                    reader.consume(end+1);
                } else {
                    panic!("Missing newline");
                }
            }
        } else {
            // didn't find the separator
            stash.extend_from_slice(buf);
            let consumed = buf.len();
            reader.consume(consumed);
            buf = reader.fill_buf().unwrap();
            let mut it = buf.iter().enumerate();
            if let Some((sep, _)) = it.find(|(_, &b)| b == b';') {
                if let Some((end, _)) = it.find(|(_, &b)| b == b'\n') {
                    let (name, rest) = buf.split_at(sep);
                    stash.extend_from_slice(name);
                    let (val, _) = rest[1..].split_at(end - sep - 1);

                    let v = parse_decimal(val);

                    // dbg!(String::from_utf8_lossy(name), v);
                    insert_or_update(&mut table, &stash, v);
                    reader.consume(end+1);
                } else {
                    // didn't get to the newline
                    stash.extend_from_slice(buf);
                    let consumed = buf.len();
                    reader.consume(consumed);
                    buf = reader.fill_buf().unwrap();
                    let mut it = buf.iter().enumerate();
                    if let Some((end, _)) = it.find(|(_, &b)| b == b'\n') {
                        stash.extend_from_slice(&buf[..end]);
                        let (name, rest) = stash.split_at(sep);
                        let val = &rest[1..];
                        let v = parse_decimal(val);

                        // dbg!(String::from_utf8_lossy(name), v);
                        insert_or_update(&mut table, name, v);
                        reader.consume(end+1);
                    } else {
                        panic!("Missing newline");
                    }
                }
            }
        }
        stash.clear();
    }

    table
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


fn report(table: &Table) -> Result<(), Box<dyn Error>> {
    let mut stdout = std::io::stdout().lock();
    write!(stdout, "{{")?;

    let table: BTreeMap<String, &Record> = table.iter().map(|(k, v)| {
        let city_str = String::from_utf8_lossy(k);
        (city_str.to_string(), v)
    })
        .collect();
    for (city, record) in table.into_iter() {
        write!(stdout, "{city}={:.1}/{:.1}/{:.1}, \n", record.min, record.mean(), record.max)?;
    }
    writeln!(stdout, "}}")?;
    Ok(())
}
