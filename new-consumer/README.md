Quickstart
----------

Before running the examples, make sure that Zookeeper and Kafka are running. In what follows, 
we assume that Zookeeper and Kafka are started with the default settings.

    # Start Zookeeper
    $ bin/zookeeper-server-start etc/kafka/zookeeper.properties

    # Start Kafka
    $ bin/kafka-server-start etc/kafka/server.properties

   
First create a topic called test:

    # Create test topic
    $ bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 \
      --partitions 1 --topic test

Then start the new-consumer example: 
    
    $ mvn exec:java -Dexec.mainClass="io.confluent.examples.consumer.NewConsumerExample" \
     -Dexec.args="5"

Finally start the console producer:

    $ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
    This is a message
    This is another message