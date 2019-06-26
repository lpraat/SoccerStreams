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
        producer.poll(0)
        producer.produce(topic, event_str.encode('ascii'), callback=delivery_report)

        count = 0
        # Send the rest
        for line in half:
            event_str = line[:-1]
            produced = False

            while not produced:
                try:
                    producer.poll(0)
                    producer.produce(topic, event_str.encode('ascii'), callback=delivery_report)
                    produced = True
                    count += 1
                except BufferError:
                    producer.poll(0.001)

            if count % 200000 == 0:
                print(f"Produced {count * 1e-3 :.0f}k events")

        producer.flush()


if __name__ == '__main__':
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    play(producer, 'first_half.txt', 'first_half_full')
    #play(producer, 'second_half.txt', 'second_half_full')
