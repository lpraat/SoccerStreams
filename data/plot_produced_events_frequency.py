import time

import matplotlib.pyplot as plt
from confluent_kafka import Producer


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))


def plot_events_produced_frequency(data):
    second = 0
    seconds = []
    num_events = []
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    with open(data, 'r') as first_half:
        num_events_produced = 0

        delta = 0
        curr = time.time()

        for line in first_half:
            event_str = line[:-1]

            produced = False

            while not produced:
                try:
                    producer.poll(0)
                    producer.produce('fake-topic', event_str.encode('ascii'), callback=delivery_report)
                    produced = True
                    num_events_produced += 1
                except BufferError:
                    producer.poll(0.001)

            now = time.time()
            delta += now - curr
            curr = time.time()
            if delta >= 1:
                seconds.append(second)
                num_events.append(num_events_produced)
                num_events_produced = 0
                second = second + 1
                delta = 0

            if second == 60*5:
                break

    fig = plt.figure()
    plt.xlabel('Second')
    plt.ylabel('Num events')
    ax = fig.add_subplot(1, 1, 1)
    ax.set_xlim(0, 60*5)
    ax.set_ylim(0, 200000)
    ax.plot(seconds, num_events)
    plt.show()


if __name__ == '__main__':
    plot_events_produced_frequency('first_half.txt')
