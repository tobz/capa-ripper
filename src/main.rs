extern crate glob;
extern crate serde;
extern crate serde_json;
extern crate flate2;
extern crate rayon;

use std::path;
use serde_json::Value;
use serde::Deserialize;
use rayon::prelude::*;

mod compressor;
mod json;

type BI<'a, A> = Box<Iterator<Item = A> + 'a>;

fn data_dir_for(state: &str, set: &str, table: &str) -> String {
    format!("data/{}/{}/{}", state, set, table)
}

fn files_for_format<'a>(set: &str, table: &str, ext: &str) -> BI<'a, path::PathBuf> {
    let source_root = data_dir_for(ext, set, table);
    let glob = source_root.clone() + "/*." + ext;
    let mut vec: Vec<path::PathBuf> = ::glob::glob(&glob)
        .unwrap()
        .map(|p| p.unwrap().to_owned())
        .collect();
    vec.sort();
    Box::new(vec.into_iter())
}

fn get_json_gz_files<'a, 'b>(set: &str, table: &str) -> BI<'a, BI<'b, Value>>
{
    let compressor = compressor::Compressor::get("gz");
    Box::new(files_for_format(set, table, "gz").into_iter().map(move |f| {
        Box::new(json::JsonReader::new(compressor.read_file(f))) as BI<'b, Value>
    }))
}

fn get_json_string<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.find(key).and_then(|s| s.as_str())
}

fn assert_json_string(v: &Value, key: &str, value: &str) -> bool {
    get_json_string(v, key).and_then(|s| Some(s == value)).unwrap_or(false)
}

fn is_problem_check_event(v: &Value) -> bool {
    assert_json_string(v, "event_type", "problem_check") && assert_json_string(v, "event_source", "server")
}

fn main() {
    // Pull in all of the tracking logs we have in the small set, creating a JSON reader for each.
    let files = get_json_gz_files("large", "tracking");

    // Filter each file to only get problem_check events that came from the server.
    let problem_check_event_count = files.collect::<Vec<BI<'static, Value>>>().as_ref()
        .par_iter::<ParallelIterator<Item = BI<'static, Value>>>()
        .flat_map(|file|
            file.filter(|event| is_problem_check_event(event)))
        .count();

    //println!("found {} problem_check events!", problem_check_events.count())
}
