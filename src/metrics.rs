use std::{convert::Infallible, net::SocketAddr, str::from_utf8, vec::Vec};

use lazy_static::lazy_static;
use prometheus::{self, Encoder, IntCounter, register, register_int_counter, TextEncoder};

#[cfg(all(target_os = "linux"))]
use prometheus::process_collector::ProcessCollector;

use warp::Filter;

use crate::Error;

// initialize the prometheus metrics
lazy_static! {
    // pub static ref RABBITMQ_MESSAGES_SENT_COUNTER: IntCounter = register_int_counter!(
    //     "rabbitmq_messages_sent",
    //     "Number of messages sent to RabbitMQ"
    // ).unwrap();

    pub static ref TOTAL_BYTES_PROCESSED: IntCounter = register_int_counter!(
        "total_bytes_processed",
        "Total bytes sent to downstream sync"
    ).unwrap();

    pub static ref TOTAL_MESSAGES_PROCESSED: IntCounter = register_int_counter!(
        "total_messages_processed",
        "Total number of messages processed"
    ).unwrap();
}

pub async fn run_warp(prom_addr: SocketAddr) -> Result<(), Error> {
    let exporter = warp::path!("metrics").and_then(serve_metrics);
    warp::serve(exporter).run(prom_addr).await;
    Ok(())
}

pub async fn serve_metrics() -> Result<impl warp::Reply, Infallible> {
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    let metric_families = prometheus::gather();
    encoder
        .encode(&metric_families, &mut buf)
        .expect("encoding prometheus metrics as text");
    let text = from_utf8(&buf).expect("converting bytes to utf8");
    Ok(text.to_owned())
}
