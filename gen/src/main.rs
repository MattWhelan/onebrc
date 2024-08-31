use std::env::args;
use std::io::{BufWriter, Write};
use std::process::exit;
use gen;

fn main() {
    let count: usize = if let [_, count_str, ..] = &args().collect::<Vec<_>>()[..] {
        count_str.parse().expect("invalid count")
    } else {
        println!("Usage: gen <count>");
        exit(1);
    };

    let stdlock = std::io::stdout().lock();

    let mut bufout = BufWriter::new(stdlock);
    gen::gen(count)
        .for_each(|(city, temp)| {
            writeln!(bufout, "{city};{temp:.1}").unwrap();
        })
}
