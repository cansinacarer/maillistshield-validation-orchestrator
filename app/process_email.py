import requests

from app.config import RABBITMQ_DEFAULT_VHOSTS, VALIDATION_WORKERS, VALIDATOR_API_KEY
from app.utilities.logging import logger
from app.utilities.rabbitmq import QueueAgent


class EmailProcessor:
    def __init__(self):
        self.next_worker = 0
        # Processor will use the second vhost for RabbitMQ
        # This is the queue we publish the results after processing
        self.queue_agent = QueueAgent(rabbitmq_vhost=RABBITMQ_DEFAULT_VHOSTS[1])

        if not self.queue_agent:
            logger.error("Queue agent is not initialized.")
            raise Exception("Not connected to RabbitMQ.")

    def get_next_worker(self):
        """
        Get the next validation worker in a round-robin fashion.

        Validation workers are defined in the VALIDATION_WORKERS
        environment variable as a comma separated list.
        """
        self.next_worker = (self.next_worker + 1) % len(VALIDATION_WORKERS)
        worker = VALIDATION_WORKERS[self.next_worker]
        return worker

    def validate_email(self, message):
        """
        Send the email to the next validation worker and return the response.
        """
        # Grab email and queueName from the message
        email, queueName = message.get("email"), message.get("queueName")

        worker = self.get_next_worker()
        response = requests.post(
            f"{worker}/validate",
            json={"email": email, "api_key": VALIDATOR_API_KEY},
        )
        return response.json()

    def process_message(self, message):
        """Grab the email from the message, validate it,
        and publish the result to the appropriate queue.

        Args:
            message (dict): Incoming message from RabbitMQ queue for emails pending validation.

        Returns:
            True: If processing and publishing was successful.
            False: If there was an error during processing or publishing.
        """
        # Grab email and queueName from the message
        email, queue_name = message.get("email"), message.get("queueName")
        if not email:
            logger.error(f"No email found in message: {message}")
            return False
        if not queue_name:
            logger.error(f"No queueName found in message: {message}")
            return False

        try:
            # Validate the email and get the result
            validation_result = self.validate_email(email)

            # Publish the validation result to the queue named
            # the same as the queue of the incoming message
            self.queue_agent.publish_message(
                queue_name=queue_name, message_body=validation_result
            )

            logger.info(f"Validation result for {email}: {validation_result}")

            return True
        except Exception as e:
            logger.error(f"Error processing email {email}: {e}")
            return False
