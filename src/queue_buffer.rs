use crate::metrics::{TOTAL_BYTES_PROCESSED, TOTAL_MESSAGES_PROCESSED};
use crate::{ChangePayload, Error, MessageQueue};
use tracing::debug;

impl QueueBuffer {
    pub fn new(queue: MessageQueue, max_size: usize) -> QueueBuffer {
        QueueBuffer {
            max_size,
            buffer: Vec::new(),
            queue,
        }
    }

    pub async fn push(&mut self, payload: ChangePayload) -> Result<(), Error> {
        debug!("pushing payload to buffer {:?}", payload);
        self.buffer.push(payload);

        if self.buffer.len() >= self.max_size {
            self.flush().await?;
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), Error> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let content = serde_json::to_string(&self.buffer)?;
        let bytes = content.into_bytes();
        let size: u64 = bytes.len() as u64;
        let publish_result = self.queue.publish(bytes).await;

        TOTAL_BYTES_PROCESSED.inc_by(size);
        TOTAL_MESSAGES_PROCESSED.inc_by(self.buffer.len() as u64);

        self.buffer.clear();
        publish_result
    }
}

pub struct QueueBuffer {
    max_size: usize,
    queue: MessageQueue,
    buffer: Vec<ChangePayload>,
}

#[cfg(test)]
mod queue_buffer_tests {
    use crate::model::ChangePayload;
    use crate::queue_buffer::QueueBuffer;
    use crate::queues::MockMessageQueue;
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn should_send_to_queue() {
        let mut queue_mock = MockMessageQueue::new();

        queue_mock.expect_publish().returning(|_| Ok(()));

        let mut queue_buffer = QueueBuffer::new(Arc::new(queue_mock), 2);
        let change = ChangePayload::new(
            "test_table".to_string(),
            json!(["test_id"]).to_string(),
            json!({ "test": "test" }).to_string(),
        )
        .unwrap();

        queue_buffer.push(change).await.unwrap();

        assert_eq!(queue_buffer.buffer.len(), 1);
    }

    #[tokio::test]
    async fn should_auto_flush_filled() {
        let mut queue_mock = MockMessageQueue::new();
        let change = ChangePayload::new(
            "test_table".to_string(),
            json!(["test_id"]).to_string(),
            json!({ "test": "test" }).to_string(),
        )
        .unwrap();

        let change_str = serde_json::to_string(&vec![change.clone()]).unwrap();

        queue_mock
            .expect_publish()
            .withf(move |bytes: &Vec<u8>| {
                let bytes_str = String::from_utf8(bytes.clone()).unwrap();
                bytes_str == change_str
            })
            .times(1)
            .returning(|_| Ok(()));

        let mut queue_buffer = QueueBuffer::new(Arc::new(queue_mock), 1);

        queue_buffer.push(change).await.unwrap();

        assert_eq!(queue_buffer.buffer.len(), 0);
    }
}
