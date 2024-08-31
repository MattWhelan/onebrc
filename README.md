# onebrc

The [One Billion Row Challenge](https://1brc.dev/) asks a program to parse a simple (but very large) text file as quickly as possible.

This implementation manages it in about 3.1s with a hot cache on an M2 Macbook Pro, using only safe Rust.

The `onebrc` binary crate contains the implementation. `gen` is a Rust generator for the input file, for convenience. 
It takes a couple minutes to run; be sure to redirect output to a file.