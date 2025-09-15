import json

from app.utilities.rabbitmq import QueueAgent
from app.utilities.logging import logger
from app.config import ROWS_PER_ROUND
from app.process_email import EmailProcessor

queue_agent = QueueAgent()
email_processor = EmailProcessor()


while True:
    discovered_queues = queue_agent.list_all_queues()
    if len(discovered_queues) == 0:
        logger.debug("No queues found. Retrying...")
        continue
    logger.debug(f"Discovered Queues: {discovered_queues}")

    # Iterate through each discovered queue
    for i, queue in enumerate(discovered_queues):
        # Iterate as many times as the message-per-round setting
        for _ in range(ROWS_PER_ROUND):
            logger.debug(
                f"Attempting to read {ROWS_PER_ROUND} messages from queue: {queue}"
            )
            message = queue_agent.get_message(queue_name=queue)
            if message:
                # If the processor was able to complete validation and publishing to the result queue
                if email_processor.process_message(message):
                    queue_agent.acknowledge_message(message)
                else:
                    queue_agent.reject_message(message, requeue=True)

                logger.debug(
                    f"Row {message['rowNumber']}/{message['totalRows']} processed from queue {queue}: {json.dumps(message, indent=2)}"
                )
            else:
                logger.debug(f"No messages in queue: {queue}.")

            if i == len(discovered_queues) - 1:
                logger.debug(
                    f"Round of processing {ROWS_PER_ROUND} messages from all queues is complete."
                )
