# Mail List Shield - Email Validation Worker

This service orchestrates email validation with the following tasks:

Consumes the individual validation tasks from the RabbitMQ queues,
Send the email to a worker, if the result is invalid, send it to the next worker,
Use the best results to build the results files,
Update the file progress by writing to the last_pick_row field of the Batch Jobs table.

See the [main repository](https://github.com/cansinacarer/maillistshield-com) for a complete list of other microservices.
