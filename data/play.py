import time

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open('first_half.txt', 'r') as first_half:

    # Send the first event
    event_str = first_half.readline()[:-1]
    start_event_time = int(event_str.split(",")[1])
    producer.send('test', event_str.encode('ascii'))
    elapsed = time.time()

    # Send the rest
    for line in first_half:
        event_str = line[:-1]
        timestamp = int(event_str.split(",")[1])

        real_diff = (timestamp - start_event_time) * 10 ** -12
        diff = time.time() - elapsed

        while diff < real_diff:
            diff = time.time() - elapsed

        if diff > real_diff + 0.01:
            elapsed += (diff - real_diff)

        producer.send('test', event_str.encode('ascii'))

    producer.flush()
