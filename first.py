

def accumulate(state, line):
    """
    Accumulates state, line by line

    >>> s = {}
    >>> accumulate(s, "2016-12-01T07:30:00 46")
    >>> accumulate(s, "2016-12-01T08:00:00 42")
    >>> s
    {'total-cars': 88}
    """

    # Parse
    (date, cars) = line.split(' ')
    cars = int(cars)

    # Sum
    state['total-cars'] = state.get('total-cars', 0) + cars


def run(filename):
    with open(filename) as f:
        state = {}
        for line in f:
            accumulate(state, line)
        print(state)


if __name__ == '__main__':
    run("data.file")
