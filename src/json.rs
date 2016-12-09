use std::io;
use std::io::BufRead;
use serde_json;
use serde_json::value::Value;
use serde::Deserialize;

pub struct JsonReader<R>
    where R: io::Read
{
    stream: io::Lines<io::BufReader<R>>,
}

impl<R> JsonReader<R>
    where R: io::Read
{
    pub fn new(r: R) -> JsonReader<R> {
        JsonReader {
            stream: io::BufReader::new(r).lines(),
        }
    }
}

impl<R> Iterator for JsonReader<R>
    where R: io::Read
{
    type Item = Value;

    fn next(&mut self) -> Option<Value> {
        match self.stream.next() {
            Some(res) => match res {
                Ok(line) => serde_json::from_str::<Value>(&*line).ok(),
                Err(_) => None,
            },
            None => None,
        }
    }
}
