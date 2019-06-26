import csv
import math
import datetime


def build_target_possession(player_file, till):
    possessions = []
    to_skip = 1  # first line
    with open(player_file) as csv_file:
        reader = csv.reader(csv_file, delimiter=';')

        for row in reader:
            if to_skip:
                to_skip -= 1
                continue

            if not row:
                continue

            if row[0] == 'Statistic:' or row[0] == '':
                break

            t = datetime.datetime.strptime(row[2], "%H:%M:%S.%f")
            t = float(t.minute * 60 + t.hour * 60 * 60 + t.second) * math.pow(10, 12) + t.microsecond * math.pow(10, 6)

            if t <= till:
                possessions.append(t)

    possession_time = 0

    # always match begin end
    if len(possessions) % 2 != 0:
        possessions = possessions[-1:]

    for i in range(0, len(possessions) - 1, 2):
        possession_time += possessions[i + 1] - possessions[i]

    return possession_time * 10 ** -12


def build_target_possessions_first_half():
    players = (
        "Nick Gertje",
        "Dennis Dotterweich",
        "Willi Sommer",
        "Philipp Harlass",
        "Roman Hartleb",
        "Erik Engelhardt",
        "Sandro Schneider",

        "Leon Krapf",
        "Kevin Baer",
        "Luca Ziegler",
        "Ben Mueller",
        "Vale Reitstetter",
        "Christopher Lee",
        "Leon Heinze",
        "Leo Langhans",
    )

    possessions = {}
    for player in players:
        file_name = f"oracle/Ball Possession/1st Half/{player}.csv"
        # [(12397999951273772 - 10753295594424116L) * 10 ** -12 + 3.092 + 0.9888] * 10**12
        player_possession = build_target_possession(file_name, 1648785156849656)
        possessions[player] = player_possession

    return possessions


def build_target_possessions_second_half():
    players = (
        "Nick Gertje",
        "Dennis Dotterweich",
        "Niklas Welzlein",
        "Willi Sommer",
        "Philipp Harlass",
        "Roman Hartleb",
        "Erik Engelhardt",
        "Sandro Schneider",

        "Leon Krapf",
        "Kevin Baer",
        "Luca Ziegler",
        "Ben Mueller",
        "Vale Reitstetter",
        "Christopher Lee",
        "Leon Heinze",
        "Leo Langhans",
    )

    possessions = {}
    for player in players:
        file_name = f"oracle/Ball Possession/2nd Half/{player}.csv"
        # [(14879639049922641 - 13086639146403495) * 10 ** -12 + 0.455 + 0.8482] * 10**12
        player_possession = build_target_possession(file_name, 1794303103519146)
        possessions[player] = player_possession

    return possessions


def compute_errors_first_half():

    target_posssessions = build_target_possessions_first_half()
    predicted_possessions = {}

    with open('../results/to_validate/first_half/ball_possession.txt') as f:
        possessions = []
        for row in f:
            possessions.append(row)

        possessions = possessions[::-1]
        already_checked = set()

        for event in possessions:
            event_split = event.split(",")
            player = event_split[1]
            time = int(event_split[2])

            if player not in already_checked:
                predicted_possessions[player] = time * 10**-12
                already_checked.add(player)

    errors = {}

    for player, possession in target_posssessions.items():

        # I'm too lazy to rename where needed
        if player == 'Willi Sommer':
            player = 'Wili Sommer'

        if player not in predicted_possessions:
            continue

        errors[player] = abs(possession - predicted_possessions[player])

    return errors

def compute_errors_second_half():

    target_posssessions = build_target_possessions_second_half()
    predicted_possessions = {}

    with open('../results/to_validate/second_half/ball_possession.txt') as f:
        possessions = []
        for row in f:
            possessions.append(row)

        possessions = possessions[::-1]
        already_checked = set()

        for event in possessions:
            event_split = event.split(",")
            player = event_split[1]
            time = int(event_split[2])

            if player not in already_checked:
                predicted_possessions[player] = time * 10**-12
                already_checked.add(player)

    errors = {}

    for player, possession in target_posssessions.items():

        # I'm too lazy to rename where needed
        if player == 'Willi Sommer':
            player = 'Wili Sommer'

        if player not in predicted_possessions:
            continue

        errors[player] = abs(possession - predicted_possessions[player])

    return errors
