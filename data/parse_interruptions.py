import csv
import datetime
import math


def parse_interruptions(half, half_start, half_interruption_end, mode='subtract'):
    interruptions = []
    to_skip = 3  # first line, first interruption start and end
    begin = True
    with open(half) as csv_file:
        reader = csv.reader(csv_file, delimiter=';')

        for row in reader:
            if to_skip:
                to_skip -= 1
                continue

            if not row:
                continue
            if row[0] == 'Statistic:' or row[0] == '':
                break

            struct_time = datetime.datetime.strptime(row[2], "%H:%M:%S.%f")
            struct_time = float(struct_time.minute * 60 + struct_time.hour * 60 * 60 + struct_time.second) * math.pow(
                10, 12) + (struct_time.microsecond) * math.pow(10, 6)

            if mode == 'subtract':
                timestamp = int((struct_time - half_interruption_end) + half_start)
                print(half_interruption_end)
                print(timestamp)
                print((timestamp - half_start) * 10 ** -12)
            else:
                timestamp = int(struct_time) + half_start
                print(timestamp)
                print((timestamp - half_start) * 10 ** -12)

            id = 1 if begin else 0
            interruptions.append(f'{id},{timestamp},0,0,0,0,0,0,0,0,0,0,0')

            begin = not begin

    print(f"Parsed {len(interruptions)} interruptions")
    print(interruptions)
    return interruptions


def parse_first_half_interruptions():
    # 4080800000000 == (3.092 + 0.9888) * 10**12
    return parse_interruptions('oracle/Game Interruption/1st Half.csv', 10753295594424116, 4080800000000, 'subtract')


def parse_second_half_interruptions():
    # TODO add estimated delay
    return parse_interruptions('oracle/Game Interruption/2nd Half.csv', 13086639146403495, 455000000000, 'subtract')


if __name__ == '__main__':
    parse_first_half_interruptions()
