use crate::Error;
use crate::RABBITMQ_MESSAGES_SENT_COUNTER;

use lapin::{
    options::{
        BasicPublishOptions, 
        QueueDeclareOptions,
    }, 
    Channel,
    BasicProperties,
    types::FieldTable,
};
use async_trait::async_trait;

#[async_trait]
pub trait MessageQueue {
    async fn publish(&self, data: Vec<u8>) -> Result<(), Error>;
}

pub struct RabbitMQ {
    channel: Channel,
    queue_name: String,
}

impl RabbitMQ {
    pub fn new(channel: Channel, queue_name: String) -> Self {
        Self {
            channel: channel,
            queue_name: queue_name,
        }
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

pub async fn init_rabbitmq_channel(message_queue: &RabbitMQ) -> Result<(), Error> {
    let _queue = message_queue.channel.queue_declare(
        &message_queue.queue_name,
        QueueDeclareOptions::default(),
        FieldTable::default(),
    ).await?;

    Ok(())
}