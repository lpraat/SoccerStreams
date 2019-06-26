from parse_interruptions import parse_first_half_interruptions, parse_second_half_interruptions


def split_dataset(full_game_path, first_half_path, second_half_path):
    """
    Splits the full game data into first half and second half data
    and adds the game interruption events.
    """
    first_half_start = 10753295594424116

    # end because there is no ball data
    first_half_no_ball = 12398000000000000
    # first_half_end = 12557295594424116

    second_half_start = 13086639146403495
    second_half_end = 14879639146403495

    first_half_interruptions = parse_first_half_interruptions()
    second_half_interruptions = parse_second_half_interruptions()

    first_half_interruptions = [(first_half_interruptions[i], int(first_half_interruptions[i].split(",")[1])) for i in range(len(first_half_interruptions))]
    second_half_interruptions = [(second_half_interruptions[i], int(second_half_interruptions[i].split(",")[1])) for i in range(len(second_half_interruptions))]

    with open(full_game_path, 'r') as full_game,  \
         open(first_half_path, 'w') as first_half, \
         open(second_half_path, 'w') as second_half: \

        first_half_num_events = 0
        first_half_interruption_events = 0
        second_half_num_events = 0
        second_half_interruption_events = 0
        discarded_num_events = 0

        for line in full_game:
            event = line[:-1]
            timestamp = int(event.split(",")[1])

            if first_half_start <= timestamp <= first_half_no_ball:

                while len(first_half_interruptions) > 0 and timestamp > first_half_interruptions[0][1]:
                    interruption = first_half_interruptions.pop(0)
                    if first_half_start <= interruption[1] <= first_half_no_ball:
                        first_half_interruption_events += 1
                        first_half.write(interruption[0])
                    else:
                        raise RuntimeError()

                first_half.write(line)
                first_half_num_events += 1

            elif second_half_start <= timestamp <= second_half_end:

                while len(second_half_interruptions) > 0 and timestamp > second_half_interruptions[0][1]:
                    interruption = second_half_interruptions.pop(0)
                    if second_half_start <= interruption[1] <= second_half_end:
                        second_half_interruption_events += 1
                        second_half.write(interruption[0])
                    else:
                        raise RuntimeError()

                second_half.write(line)
                second_half_num_events += 1
            else:
                discarded_num_events += 1

        print("Splitted dataset")
        print(f"Discarded {discarded_num_events}")
        print(f"Number of events in the first half {first_half_num_events}")
        print(f"Number of interruption events in the first half {first_half_interruption_events}")
        print(f"Number of events in the second half {second_half_num_events}")
        print(f"Number of interruption events in the second half {second_half_interruption_events}")


if __name__ == '__main__':
    split_dataset('full-game', 'first_half.txt', 'second_half.txt')
