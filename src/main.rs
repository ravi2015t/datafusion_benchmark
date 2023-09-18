use std::error::Error;
use std::path::Path;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::json;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
use env_logger::Env;
use std::fs;
use std::sync::Arc;
use tokio::time::Instant;
// use std::time::Instant;

use datafusion::arrow::array::Float32Array;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
// #[tokio::main]
#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
    let now = Instant::now();

    let mut query_tasks = Vec::new();
    let num_requests = 10;

    for i in 1..num_requests {
        query_tasks.push(tokio::spawn(compute(i)));
    }

    // tokio::join!(compute(1), compute(2));
    for task in query_tasks {
        task.await.expect("waiting failed");
    }
    // writer.finish()?;
    let end = Instant::now();
    println!("Total Time elapased {:?}", end - now);
    Ok(())
}

async fn compute(id: u16) -> Result<(), DataFusionError> {
    // create local session context
    let ctx = SessionContext::new();
    // register parquet file with the execution context
    ctx.register_parquet(
        "ph",
        &format!("pensionHistory/{}/file.parquet", id),
        ParquetReadOptions::default(),
    )
    .await?;

    let load_all_data_query = "SELECT * from ph";
    let all_data = ctx.sql(load_all_data_query).await?;
    let all_data = all_data.collect().await.unwrap();

    log::info!("Registered all data for task {}", id);
    let schema = all_data[0].schema(); // Assuming all batches have the same schema

    let table = MemTable::try_new(schema, vec![all_data])?;
    let table_name = format!("pension_history_{}", id);
    ctx.register_table(&table_name, Arc::new(table))?;

    log::info!("Registered all data to memory for task {}", id);
    let filename = format!("results.json");
    let path = Path::new(&filename);
    let file = fs::File::create(path)?;
    let mut writer = json::LineDelimitedWriter::new(file);
    let mut query_tasks = Vec::new();

    let ctx = Arc::new(ctx);

    for i in 1..49 {
        let ctx = ctx.clone();
        let table_name = table_name.clone();
        let task = tokio::spawn(async move {
            let query = format!(
                "SELECT sum({}.amount{}) as calc{} FROM {}",
                table_name, i, i, table_name
            );
            let df = ctx.sql(&query).await.expect("Failed to get dataframe");
            df.collect().await.expect("Failed to show");
            // print the results
            // let result = df.collect().await.unwrap();

            // for rec in result {
            //     writer.write(&rec).expect("Failed to write");
            // }
        });
        query_tasks.push(task);
    }

    for i in 1..48 {
        let ctx = ctx.clone();
        let table_name = table_name.clone();
        let task = tokio::spawn(async move {
            let query = format!(
                "SELECT sum({}.number{}) as calc{} FROM {}",
                table_name, i, i, table_name
            );
            let df = ctx.sql(&query).await.expect("Failed to get dataframe");
            df.collect().await.expect("Failed to show");
            // print the results
            // let result = df.collect().await.unwrap();

            // for rec in result {
            //     writer.write(&rec).expect("Failed to write");
            // }
        });
        query_tasks.push(task);
    }

    log::info!("Finished pushing all tasks to vec for task {}", id);
    // tokio::join!(query_tasks);
    for task in query_tasks {
        task.await.expect("waiting failed");
    }
    // writer.finish()?;
    let end = Instant::now();
    // println!("Time elapased {:?}", end - now);
    log::info!("Finished executing for task {}", id);
    Ok(())
}
