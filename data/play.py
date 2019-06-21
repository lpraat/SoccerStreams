from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open('first_half.txt', 'r') as first_half:
    # Send the first event
    event_str = first_half.readline()[:-1]
    start_event_time = int(event_str.split(",")[1])
    producer.send('test', event_str.encode('ascii'))

    # Send the rest
    for line in first_half:
        event_str = line[:-1]
        timestamp = int(event_str.split(",")[1])
        producer.send('test', event_str.encode('ascii'))

    producer.flush()
