use async_trait::async_trait;
use std::io::Write;
use tracing::debug;

use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use tokio_amqp::LapinTokioExt;

use crate::Error;

#[async_trait]
pub trait MessageQueue {
    async fn publish(&self, data: Vec<u8>) -> Result<(), Error>;
    // async fn flush(&self) -> Result<(), Error>;
}

pub struct StdoutDump {}

impl StdoutDump {}

#[async_trait]
impl MessageQueue for StdoutDump {
    async fn publish(&self, data: Vec<u8>) -> Result<(), Error> {
        std::io::stdout().write_all(&data)?;
        Ok(())
    }
}

pub struct RabbitMQ {
    channel: Channel,
    queue_name: String,
}

impl RabbitMQ {
    pub async fn new(mq_addr: String, queue_name: String) -> Result<Self, Error> {
        let conn =
            Connection::connect(&mq_addr, ConnectionProperties::default().with_tokio()).await?;
        let channel = conn.create_channel().await?;

        let _queue = channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(Self {
            channel,
            queue_name,
        })
    }
}

#[async_trait]
impl MessageQueue for RabbitMQ {
    async fn publish(&self, data: Vec<u8>) -> Result<(), Error> {
        let publish_result = self
            .channel
            .basic_publish(
                "",
                &self.queue_name,
                BasicPublishOptions::default(),
                data,
                BasicProperties::default(),
            )
            .await?
            .await;

        // RABBITMQ_MESSAGES_SENT_COUNTER.inc();

        match publish_result {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::LapinError(e)),
        }
    }
}

pub struct SQSQueue {
    queue_url: String,
    client: aws_sdk_sqs::Client,
}

impl SQSQueue {
    pub async fn new(queue_url: String) -> Result<Self, Error> {
        let config = aws_config::load_from_env().await;
        let client = aws_sdk_sqs::Client::new(&config);

        Ok(SQSQueue { queue_url, client })
    }
}

#[async_trait]
impl MessageQueue for SQSQueue {
    async fn publish(&self, data: Vec<u8>) -> Result<(), Error> {
        debug!("Publishing to sqs");

        self.client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(std::str::from_utf8(&data)?)
            .send()
            .await?;

        Ok(())
    }
}
