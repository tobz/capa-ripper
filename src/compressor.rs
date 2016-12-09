extern crate flate2;

use std::io;
use std::fs::File;
use std::path;

use flate2::Compression;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;

#[derive(Clone,Copy)]
pub enum Compressor {
    Gz,
}

impl Compressor {
    pub fn get(name: &str) -> Compressor {
        match name {
            "gz" => Compressor::Gz,
            _ => panic!("unknown compressor {}", name),
        }
    }

    pub fn for_format(name: &str) -> Compressor {
        let tokens: Vec<String> = name.split("-").map(|x| x.to_owned()).collect();
        Compressor::get(if tokens.len() == 1 {
            "none"
        } else {
            &*tokens[1]
        })
    }

    pub fn read_file<P: AsRef<path::Path>>(&self, name: P) -> Box<io::Read + Send> {
        self.decompress(File::open(name).unwrap())
    }

    pub fn write_file<P: AsRef<path::Path>>(&self, name: P) -> Box<io::Write> {
        self.compress(File::create(name).unwrap())
    }

    pub fn compress<W: io::Write + Send + 'static>(&self, w: W) -> Box<io::Write> {
        match *self {
            Compressor::Gz => {
                Box::new(GzEncoder::new(w, Compression::Default))
            }
        }
    }

    pub fn decompress<R: io::Read + Send + 'static>(&self, r: R) -> Box<io::Read + Send> {
        match *self {
            Compressor::Gz => Box::new(GzDecoder::new(r).unwrap()),
        }
    }
}
