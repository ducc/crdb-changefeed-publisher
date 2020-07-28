use crate::Error;

use lazy_static::lazy_static;
use prometheus::{self, register_int_counter, Encoder, IntCounter, TextEncoder};
use std::{convert::Infallible, net::SocketAddr, str::from_utf8, vec::Vec};
use warp::Filter;

// initialize the prometheus metrics
lazy_static! {
    pub static ref RABBITMQ_MESSAGES_SENT_COUNTER: IntCounter = register_int_counter!(
        "rabbitmq_messages_sent",
        "Number of messages sent to RabbitMQ"
    )
    .unwrap();
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
