#!/bin/bash
# for quick tests to verify that the messages are available in the topic
# make it easy to swap the Kafka bootstrap servers and topic
bootstrap_servers="localhost:9092"
topic="your-topic"

# Run the kafka-console-consumer tool
/opt/homebrew/opt/kafka/bin/kafka-console-consumer --bootstrap-server "$bootstrap_servers" --topic "$topic" --from-beginning
