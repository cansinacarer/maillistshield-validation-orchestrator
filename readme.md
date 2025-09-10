# Mail List Shield - Email Validation Worker

This service orchestrates email validation with the following tasks:

Consumes the individual validation tasks from the RabbitMQ queues,
Send the email to a worker, if the result is invalid, send it to the next worker,
Use the best results to build the results files,
Update the file progress by writing to the last_pick_row field of the Batch Jobs table.

## Round-robin logic for processing user-uploaded files fairly

- While there are non-empty queues in the relevant vhost
  - Discover queues
  - for i, queue in enumerate(discovered queues)
    - get message
    - process message
    - enqueue result in the other vhost
    - acknowledge message if successful
    - ignore message if failed
    - move to next queue
    - exit when i + 1 == len(queues)

---

See the [main repository](https://github.com/cansinacarer/maillistshield-com) for a complete list of other microservices.
