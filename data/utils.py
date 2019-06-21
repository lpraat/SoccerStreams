# TODO when splitting also add game interruption events
def split_dataset(full_game_path, first_half_path, second_half_path):
    """
    Splits the full game data into first half and second half data.
    """
    first_half_start = 10753295594424116

    # end because there is no ball data
    first_half_no_ball = 12398000000000000
    # first_half_end = 12557295594424116

    second_half_start = 13086639146403495
    second_half_end = 14879639146403495

    with open(full_game_path, 'r') as full_game,  \
         open(first_half_path, 'w') as first_half, \
         open(second_half_path, 'w') as second_half: \

        first_half_num_events = 0
        second_half_num_events = 0
        discarded_num_events = 0

        for line in full_game:
            event = line[:-1]
            timestamp = int(event.split(",")[1])

            if first_half_start <= timestamp <= first_half_no_ball:
                first_half.write(line)
                first_half_num_events += 1
            elif second_half_start <= timestamp <= second_half_end:
                second_half.write(line)
                second_half_num_events += 1
            else:
                discarded_num_events += 1

        print("Splitted dataset")
        print(f"Discarded {discarded_num_events}")
        print(f"Number of events in the first half {first_half_num_events}")
        print(f"Number of events in the second half {second_half_num_events}")


if __name__ == '__main__':
    split_dataset('full-game', 'first_half.txt', 'second_half.txt')
