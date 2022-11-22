use crate::metrics::{TOTAL_BYTES_PROCESSED, TOTAL_MESSAGES_PROCESSED};
use crate::{ChangePayload, Error, MessageQueue};

impl QueueBuffer {
    pub fn new(queue: MessageQueue, max_size: usize) -> QueueBuffer {
        QueueBuffer {
            max_size,
            buffer: Vec::new(),
            queue,
        }
    }

    pub async fn push(&mut self, payload: ChangePayload) -> Result<(), Error> {
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
