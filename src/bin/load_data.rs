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
struct Args {
    file_path: String,
    sample_rows: usize,
    max_cell_chars: usize,
    column_filter: Option<String>,
}

fn parse_args() -> Args {
    let args: Vec<String> = env::args().collect();

    let mut file_path: Option<String> = None;
    let mut sample_rows = 5;
    let mut max_cell_chars = 500;
    let mut column_filter = None;

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
            "-n" | "--rows" => {
                if i + 1 < args.len() {
                    sample_rows = args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Error: -n requires a positive number");
                        std::process::exit(1);
                    });
                    i += 1;
                } else {
                    eprintln!("Error: -n requires a row count");
                    std::process::exit(1);
                }
            }
            "--full" => {
                max_cell_chars = usize::MAX;
            }
            "-c" | "--column" => {
                if i + 1 < args.len() {
                    column_filter = Some(args[i + 1].clone());
                    i += 1;
                } else {
                    eprintln!("Error: --column requires a column name");
                    std::process::exit(1);
                }
            }
            _ => {}
        }
        i += 1;
    }

    match file_path {
        Some(path) => Args {
            file_path: path,
            sample_rows,
            max_cell_chars,
            column_filter,
        },
        None => {
            eprintln!(
                "Usage: load_data -f <parquet_file> [-n <sample_rows>] [-c <column>] [--full]"
            );
            std::process::exit(1);
        }
    }
}

fn truncate_value(value: String, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value;
    }

    let mut truncated = value.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn print_separator(label: &str) {
    println!("\n==================== {} ====================", label);
}

fn cell_to_string(col: &dyn Array, row: usize) -> String {
    if col.is_null(row) {
        return "null".to_string();
    }

    if let Some(arr) = col.as_any().downcast_ref::<Utf8Array<i32>>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Utf8Array<i64>>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
        return arr.value(row).to_string();
    }

    format!("<{:?}>", col.data_type())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let file_path = args.file_path;
    println!("Loading dataset from: {}", file_path);

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
    let mut printed_rows = 0;
    println!("\n=== Sample Rows ===");
    for maybe_chunk in file_reader.by_ref() {
        let chunk = maybe_chunk?;

        let mut row_in_chunk = 0;
        while printed_rows < args.sample_rows && row_in_chunk < chunk.len() {
            print_separator(&format!("REQUEST {}", printed_rows + 1));
            for (col_idx, field) in schema.fields.iter().enumerate() {
                if args
                    .column_filter
                    .as_ref()
                    .is_some_and(|column| column != &field.name)
                {
                    continue;
                }

                let value = cell_to_string(chunk.columns()[col_idx].as_ref(), row_in_chunk);
                let value = truncate_value(value, args.max_cell_chars);
                println!("  {}: {}", field.name, value);
            }
            printed_rows += 1;
            row_in_chunk += 1;
        }

        // Get the messages column once per chunk
        let col = &chunk.columns()[messages_col_idx];

        let arr = col.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
        // 7. Iterate rows
        for row in 0..chunk.len() {
            let msg = arr.value(row); // &str, no allocation

            // Build HTTP body
            //let _ = msg;

            let http_body = format!(r#"{{"model":"read from config","messages":{}}}"#, msg);

            http_requests.push(http_body);
        }
    }
    let elapsed = start.elapsed();

    println!("\nExtracted {} HTTP messages", http_requests.len());
    println!("Total time: {:.2?}", elapsed);

    Ok(())
}
