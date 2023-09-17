use std::path::Path;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::json;
use datafusion::error::Result;
use datafusion::prelude::*;
use std::fs;
use std::time::Instant;

use datafusion::arrow::array::Float32Array;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    let now = Instant::now();
    // create local session context
    let ctx = SessionContext::new();
    // register parquet file with the execution context
    ctx.register_parquet(
        "ph",
        &format!("pension_history.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    let filename = format!("results.json");
    let path = Path::new(&filename);
    let file = fs::File::create(path)?;
    let mut writer = json::LineDelimitedWriter::new(file);

    //Aggregated query
    // let mut query = String::new();
    // query.push_str("SELECT ");
    // for i in 1..48 {
    //     let q = format!("sum(ph.amount{}) as calc{}, ", i, i);
    //     query.push_str(&q);
    // }
    // query.push_str("sum(ph.amount49) as calc49 ");
    // query.push_str("FROM ph");
    // let df = ctx.sql(&query).await?;
    // // print the results
    // df.clone().show().await?;
    // let result = df.collect().await.unwrap();
    // for rec in result {
    //     writer.write(&rec)?;
    // }

    //Aggregated query
    let mut query = String::new();
    query.push_str("SELECT ");
    for i in 1..48 {
        let q = format!(
            "(SELECT sum(ph.amount{}) as calc{} FROM ph) as calc_table{},",
            i, i, i
        );
        query.push_str(&q);
    }
    query.push_str("(SELECT sum(ph.amount49) as calc49 FROM ph) as calc_table_49");
    // query.push_str("FROM ph");
    let df = ctx.sql(&query).await?;
    // print the results
    // df.clone().show().await?;
    let result = df.explain(false, true)?.collect().await.unwrap();
    let end = Instant::now();
    println!("Time elapased after execution {:?}", end - now);
    for rec in result {
        writer.write(&rec)?;
    }

    //Separate 100 calcs
    // execute the query

    // for i in 1..49 {
    //     let query = format!("SELECT sum(ph.amount{}) as calc{} FROM ph", i, i );
    //     let df = ctx.sql(&query).await?;
    //     // print the results
    //     let result = df.collect().await.unwrap();
    //     for rec in result {
    //         writer.write(&rec)?;
    //     }
    // }

    // for i in 1..49 {
    //     let query = format!("SELECT sum(ph.number{}) as calc{} FROM ph", i, i + 50);
    //     let df = ctx.sql(&query).await?;
    //     // print the results
    //     let result = df.collect().await.unwrap();
    //     for rec in result {
    //         writer.write(&rec)?;
    //     }
    // }

    writer.finish()?;
    let end = Instant::now();
    println!("Time elapased {:?}", end - now);
    Ok(())
}
