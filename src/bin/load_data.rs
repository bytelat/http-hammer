use std::env;
use std::fs::File;
//use std::io::BufReader;
use std::time::Instant;

use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::io::parquet::read::{self, FileReader, read_metadata};
use memmap2::MmapOptions;
//use parquet2::read::read_metadata;
//use arrow2::io::parquet::read::read_metadata;
// --------------------------------------
// Argument parser (separate function)
// --------------------------------------
fn parse_args() -> String {
    let args: Vec<String> = env::args().collect();

    let mut file_path: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-f" | "--file" => {
                if i + 1 < args.len() {
                    file_path = Some(args[i + 1].clone());
                    i += 1;
                } else {
                    eprintln!("Error: -f requires a file path");
                    std::process::exit(1);
                }
            }
            _ => {}
        }
        i += 1;
    }

    match file_path {
        Some(path) => path,
        None => {
            eprintln!("Usage: load_data -f <parquet_file>");
            std::process::exit(1);
        }
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = parse_args();
    println!("Loading OneRec dataset from: {}", file_path);
  

    // 1. Open file with mmap
    let file = File::open(&file_path)?;
    //let mut reader = BufReader::new(file);
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let mut reader = std::io::Cursor::new(&mmap[..]);



    // 2. Read Parquet metadata
    let metadata = read_metadata(&mut reader)?;
    println!("Total rows: {}", metadata.num_rows);
    println!("Row groups: {}", metadata.row_groups.len());
    
    

    // 3. Convert Parquet schema → Arrow schema
    let arrow_fields = read::schema::parquet_to_arrow_schema(metadata.schema().fields());
    let schema = Schema::from(arrow_fields);
    // Find the messages column index ONCE
    let messages_col_idx = schema
        .fields
        .iter()
        .position(|f| f.name == "messages")
        .expect("messages column not found");

    let mut http_requests: Vec<String> = Vec::with_capacity(metadata.num_rows as usize);

    // Print schema so you know the real column order
    println!("\n=== Schema ===");
    for (i, f) in schema.fields.iter().enumerate() {
        println!("{}: {} ({:?})", i, f.name, f.data_type);
    }
    println!("{:#?}", schema);
    // 4. Read first row group (you can loop all of them)
    let mut file_reader = FileReader::new(
        reader,
        metadata.row_groups.clone(),
        schema.clone(),
        None,
        None,
        None,
    );
    //let chunk = file_reader.next().unwrap()?;
    let start = Instant::now();
    //println!("Loaded {} columns", chunk.columns().len());
    // 6. Iterate over row groups
    for maybe_chunk in file_reader.by_ref() {
        let chunk = maybe_chunk?;

        // Get the messages column once per chunk
        let col = &chunk.columns()[messages_col_idx];
         
        let arr = col.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
        // 7. Iterate rows
        for row in 0..chunk.len() {
            let msg = arr.value(row); // &str, no allocation

            // Build HTTP body
            //let _ = msg;
             
            let http_body = format!(
                r#"{{"model":"OpenOneRec/OneRec-1.7B","messages":{},"max_tokens":100}}"#,
                msg
            );

            http_requests.push(http_body);
        }
        
    }
    let elapsed = start.elapsed();
    
    println!("Extracted {} HTTP messages", http_requests.len());
    println!("Total time: {:.2?}", elapsed);
    
   Ok(())
}
