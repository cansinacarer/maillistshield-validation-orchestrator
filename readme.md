# Mail List Shield - Validation Orchestrator

This service orchestrates email validation with the following tasks:

- Consumes the individual validation tasks from the RabbitMQ queues at vhost `RABBITMQ_DEFAULT_VHOSTS[0]`,
- Send the email to a worker, if the result is invalid, send it to the next worker,
- Use the best results to build the results,
- Create a queue for the file at vhost `RABBITMQ_DEFAULT_VHOSTS[1]`, if one doesn't exist,
- Publish the results in the queue at `RABBITMQ_DEFAULT_VHOSTS[1]`,
- Clean up the empty validation queues at vhost `RABBITMQ_DEFAULT_VHOSTS[0]`.

## Round-robin logic for processing user-uploaded files fairly

In order to process all user uploaded files fairly, we process the files in a round-robin fashion, processing a set number of rows from each file with each round. This is configured in the environment variable `ROWS_PER_ROUND`.

- While there are non-empty queues in the relevant vhost
  - Discover queues
  - for i, queue in enumerate(discovered queues)
    - get message
    - process message
    - enqueue result in the other vhost
    - acknowledge message if successful
    - reject message if failed
    - move to next queue
    - exit when i + 1 == len(queues)

__Job States:__

This service does not change the job state in the database. The progress of a file is tracked using the number of messages in the queue for that file at vhost `RABBITMQ_DEFAULT_VHOSTS[1]`.

---

See the [main repository](https://github.com/cansinacarer/maillistshield-com) for a complete list of other microservices.
