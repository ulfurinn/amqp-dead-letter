# amqp-dead-letter

A simple interactive program to help dig through RabbitMQ dead letter queues.

Available actions are:
* republish to original queue
* republish to original exchange
* save to a file for later inspection
* discard

## Installation

`go install github.com/ulfurinn/amqp-dead-letter@latest`

## Usage

`AMQP_URL=amqp://host:port/vhost amqp-dead-letter DL.queue.name`
