use clap::{load_yaml, App};
use crdb_changefeed_publisher::error::Error;
use crdb_changefeed_publisher::model::QueueType;
use crdb_changefeed_publisher::queues::{SQSQueue, StdoutDump};
use crdb_changefeed_publisher::{startup, MessageQueue};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // setup our logging and tracing
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("unable to set default trace subscriber");

    tracing_log::LogTracer::init()?;

    let yaml = load_yaml!("../cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let table_value = matches.value_of("table").expect("unable to get table name");
    let queue_value = QueueType::from_name(matches.value_of("queue").unwrap_or("stdout"))
        .expect("unable to get queue type");
    let cursor_frequency_value = matches.value_of("cursor-frequency").unwrap_or("10s");

    // get the environment variables
    let prom_addr_raw = std::env::var("PROMETHEUS_ADDR").unwrap_or_else(|_| "0.0.0.0:8001".into());
    let database_url = std::env::var("DATABASE_URL").expect("database url is required");
    let changefeed_id =
        std::env::var("CHANGEFEED_ID").unwrap_or_else(|_| Uuid::new_v4().to_string());

    info!("Preparing to start changefeed '{}'", changefeed_id);

    let message_queue: MessageQueue = match queue_value {
        QueueType::Stdout => Arc::new(StdoutDump {}),
        QueueType::Sqs => {
            let queue_url = std::env::var("SQS_QUEUE_URL").expect("SQS queue url is required");
            Arc::new(SQSQueue::new(queue_url).await?)
        }
    };

    startup(
        changefeed_id,
        database_url,
        table_value.to_string(),
        prom_addr_raw.parse().unwrap(),
        message_queue,
        cursor_frequency_value.to_string(),
    )
    .await
}
