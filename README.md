# scep2019

Start zookeper
- ```bin/zookeeper-server-start.sh config/zookeeper.properties```
   
Start a broker
- ```bin/kafka-server-start.sh config/server.properties```

List topics
- ```bin/kafka-topics.sh --list --bootstrap-server localhost:9092```

Run a consumer
- ```bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning```

Clear a topic
- ```bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test```

### TODO
exact environment + steps(use 3 brokers) + steps to test fault tolerance