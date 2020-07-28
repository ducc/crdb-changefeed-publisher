use crate::Error;
use crate::RABBITMQ_MESSAGES_SENT_COUNTER;

use lapin::{
    options::{
        BasicPublishOptions, 
        QueueDeclareOptions,
    }, 
    Channel,
    Connection,
    ConnectionProperties,
    BasicProperties,
    types::FieldTable,
};
use async_trait::async_trait;
use tokio_amqp::LapinTokioExt;

#[async_trait]
pub trait MessageQueue {
    async fn publish(&self, data: Vec<u8>) -> Result<(), Error>;
}

pub struct RabbitMQ {
    channel: Channel,
    queue_name: String,
}

impl RabbitMQ {
    pub async fn new(mq_addr: String, queue_name: String) -> Result<Self, Error> {
        let conn = Connection::connect(&mq_addr, ConnectionProperties::default().with_tokio()).await?; 
        let channel = conn.create_channel().await?;

        let _queue = channel.queue_declare(
            &queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        ).await?;

        Ok(Self {
            channel,
            queue_name,
        })
    }
}

#[async_trait]
impl MessageQueue for RabbitMQ {
    async fn publish(&self, data: Vec<u8>) -> Result<(), Error> {
        let publish_result = self.channel.basic_publish(
            "",
            &self.queue_name,
            BasicPublishOptions::default(),
            data,
            BasicProperties::default(),
        ).await?.await;

        RABBITMQ_MESSAGES_SENT_COUNTER.inc();

        match publish_result {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::LapinError(e)),
        }
    }
}