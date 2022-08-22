use crate::{ChangePayload, Error, MessageQueue};

impl QueueBuffer {
    pub fn new(queue: MessageQueue, max_size: usize) -> QueueBuffer {
        QueueBuffer {
            max_size,
            buffer: Vec::new(),
            queue
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
        if self.buffer.len() == 0 {
            return Ok(())
        }

        let content = serde_json::to_string(&self.buffer)?;
        let publish_result = self.queue.publish(content.into_bytes()).await;

        self.buffer.clear();
        return publish_result;
    }
}

pub struct QueueBuffer {
    max_size: usize,
    queue: MessageQueue,
    buffer: Vec<ChangePayload>
}
