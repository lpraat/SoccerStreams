import time
from confluent_kafka import Producer


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))


def play(producer, half_path, topic='test'):
    with open(half_path, 'r') as half:

        # Send the first event
        event_str = half.readline()[:-1]
        start_event_time = int(event_str.split(",")[1])
        producer.poll(0)
        producer.produce(topic, event_str.encode('ascii'), callback=delivery_report)
        elapsed = time.time()

        # Send the rest
        for line in half:
            event_str = line[:-1]
            timestamp = int(event_str.split(",")[1])

            real_diff = (timestamp - start_event_time) * 10 ** -12

            while time.time() - elapsed < real_diff:
                pass

            producer.poll(0)
            producer.produce(topic, event_str.encode('ascii'), callback=delivery_report)

        producer.flush()


if __name__ == '__main__':
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    play(producer, 'first_half.txt', 'test')
