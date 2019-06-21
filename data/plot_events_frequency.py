import matplotlib.pyplot as plt


# NOTE: events_frquency != events_produced_frequency
def plot_events_frequency(data):
    second = 0
    seconds = []
    num_events = []

    with open(data, 'r') as first_half:
        num_events_produced = 0

        start_event_time = int(first_half.readline()[:-1].split(",")[1])
        num_events_produced += 1

        for line in first_half:
            event = line[:-1].split(",")
            timestamp = int(event[1])

            real_diff = (timestamp - start_event_time) * 10 ** -12

            num_events_produced += 1

            if int(real_diff) == second + 1:
                second = second + 1
                seconds.append(second)
                num_events.append(num_events_produced)
                num_events_produced = 0

    fig = plt.figure()
    plt.xlabel('Second')
    plt.ylabel('Num events')
    ax = fig.add_subplot(1, 1, 1)
    ax.set_xlim(0, 1800)
    ax.set_ylim(0, 16000)
    ax.plot(seconds, num_events)
    plt.show()


if __name__ == '__main__':
    plot_events_frequency('first_half.txt')
    plot_events_frequency('second_half.txt')
