use std::env::args;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::mpsc;
use onebrc::Table;

fn main() -> Result<(), Box<dyn Error>> {
    if let [_, filename, ..] = &args().collect::<Vec<_>>()[..] {
        let mut infile = File::open(filename)?;

        let file_len = infile.seek(SeekFrom::End(0))?;
        let core_count: usize = std::thread::available_parallelism().unwrap().into();
        let num_chunks = core_count as u64;
        let mut splits: Vec<_> = (1..num_chunks).map(|i| i * (file_len/num_chunks))
            .map(|pos| {
                // seek forward to align with the start of a line
                infile.seek(SeekFrom::Start(pos)).unwrap();
                let mut b = [0u8; 1];
                while b[0] != b'\n' {
                    infile.read(&mut b[..]).unwrap();
                }
                infile.stream_position().unwrap()
            })
            .collect();
        drop(infile);

        splits.insert(0, 0);
        splits.push(u64::MAX);

        let mut infiles: Vec<_> = splits.windows(2)
            .map(|splits| {
                let split = splits[0];
                let len = splits[1] - splits[0];
                let mut f = File::open(filename).expect("reopen failed");
                f.seek(SeekFrom::Start(split)).unwrap();
                f.take(len)
            })
            .collect();

        let (tx, rx) = mpsc::channel::<Table>();
        std::thread::scope(|s| {
            s.spawn(move || {
                let final_table = rx.iter().reduce(|mut l, r| {
                    r.into_iter().for_each(|(k, r)| {
                        let e = l.entry(k).or_default();
                        e.merge(&r);
                    });
                    l
                })
                    .unwrap();
                onebrc::report(&final_table).unwrap();
            });


            infiles.into_iter()
                .for_each(|f| {
                    let tx = tx.clone();
                    s.spawn(move || {
                        let buf: BufReader<_> = BufReader::with_capacity(2 * 1024 * 1024, f);
                        let t = onebrc::produce_table(buf);
                        tx.send(t).expect("Send error")
                    });
                });
            drop(tx);
        });

        Ok(())
    } else {
        println!("Usage: onebrc <filename>");
        Ok(())
    }
}
