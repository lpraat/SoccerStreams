
def check_data_correctness(half, start_time):
    last_ts = 0

    with open(half, 'r') as f:
        # Send the rest
        for line in f:
            event_str = line[:-1]
            split = event_str.split(",")
            id = int(split[0])
            timestamp = int(split[1])

            if id == 1:
                print(f"Interruption begin after {(timestamp - start_time)*10**-12} seconds")

            if id == 0:
                print(f"Interruption end after {(timestamp - start_time)*10**-12} seconds")


            assert(timestamp > last_ts)
            last_ts = timestamp


if __name__ == '__main__':
    check_data_correctness('first_half.txt', 10753295594424116)
    check_data_correctness('second_half.txt', 13086639146403495)