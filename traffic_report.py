"""
Traffic report coding test
- Tushar Pokle
"""

from sys import argv
from collections import deque
from functools import reduce, partial
from itertools import chain


def min_sum_in_window(window_size, state, record):
    """
    A reducer that finds a sliding window with the minimum sum

    Result in state['min_window'] is [ <VALUE>, <START_LABEL>, <END_LABEL> ]

    Not enough data, so just pick the smallest range
    >>> min_sum_in_window(3, {}, (5, 'first'))
    {'window': deque([(5, 'first')]), 'min_window': [5, 'first', 'first']}

    Fill up window
    >>> recs = [(40, '1st'), (42, '2nd'), (35, '3rd')]
    >>> state = reduce(partial(min_sum_in_window, 3), recs, {})
    >>> state
    {'window': deque([(40, '1st'), (42, '2nd'), (35, '3rd')]), 'min_window': [117, '1st', '3rd']}

    Add one more smaller than the min_window to move the window
    >>> state = min_sum_in_window(3, state, (0, '4th'))
    >>> state
    {'window': deque([(42, '2nd'), (35, '3rd'), (0, '4th')]), 'min_window': [77, '2nd', '4th']}

    Add one larger than the min_window so that the min doesn't change.
    >>> min_sum_in_window(3, state, (1000, '5th'))
    {'window': deque([(35, '3rd'), (0, '4th'), (1000, '5th')]), 'min_window': [77, '2nd', '4th']}
    """
    (value, label) = record

    # First time around
    if 'window' not in state:
        state['window'] = deque([(value, label)])
        state['min_window'] = [value, label, label]
        return state

    min_window = state['min_window']
    window = state['window']

    if len(window) < window_size:
        # Keep accumulating until we have enough data
        window.append((value, label))
        min_window[0] += value
        min_window[2] = label
        return state

    # Slide window
    window.popleft()
    window.append((value, label))

    window_sum = sum(map(lambda t: t[0], window))
    if min_window[0] > window_sum:
        min_window[0] = window_sum    # sum(start..end)
        min_window[1] = window[0][1]  # start
        min_window[2] = window[-1][1] # end

    return state


def rank(max_ranks, state, record):
    """
    A reducer, that ranks all your records by `max_ranks` positions.

    Single value:
    >>> rank(3, {}, (1, 'first'))
    {'ranks': [(1, 'first')]}

    Top 3 in the right order
    >>> recs = [(1, '1st'), (2, '2nd'), (3, '3rd')]
    >>> reduce(partial(rank, 3), recs, {})
    {'ranks': [(3, '3rd'), (2, '2nd'), (1, '1st')]}

    Insertion and clamping:
    >>> recs = [(10, '1st'), (20, '2nd'), (30, '3rd'), (25, '4th')]
    >>> reduce(partial(rank, 3), recs, {})
    {'ranks': [(30, '3rd'), (25, '4th'), (20, '2nd')]}
    """
    if 'ranks' not in state:
        state['ranks'] = [record]
        return state

    leaders = state['ranks']
    (value, label) = record

    # Search for an insertion point
    for index, leader in enumerate(leaders):
        if value > leader[0]:
            leaders.insert(index, (value, label))
            if len(leaders) > max_ranks:
                leaders.pop()
            return state

    return state


# Sentinel value that is used by stream
END_OF_INPUT = ('GOODBYE', 0)

def stream_group_by_date(state, record):
    """
    Group by date, and report on the sum of cars per date.
    This prints output straight to stdout to keep memory
    usage in check. Perhaps more sophisticated version might write
    to a seperate file.
    """
    (datetime, cars) = record
    date = datetime.split('T')[0]
    if 'last-date' not in state:
        print('## Count of cars grouped by date')
        state['last-date'] = date
        state['date-cars'] = cars
    elif state['last-date'] != date:
        print(state['last-date'], state['date-cars'])
        state['last-date'] = date
        state['date-cars'] = cars
    else:
        state['date-cars'] += cars



def accumulate_stats(state, record):
    """
    Accumulates statistics for the report, record by record.

    Single day:
    >>> recs = [("2016-12-01T07:30:00", 46), ("2016-12-01T08:00:00", 42), END_OF_INPUT]
    >>> state = reduce(accumulate_stats, recs, {})
    ## Count of cars grouped by date
    2016-12-01 88
    >>> state['total-cars']
    88

    Multiple days:
    >>> recs = [("2016-12-01T07:30:00", 1),
    ...         ("2016-12-01T08:00:00", 1),
    ...         ("2016-12-02T00:00:00", 200),
    ...         ("2016-12-03T00:00:00", 300),
    ...         END_OF_INPUT]
    >>> state = reduce(accumulate_stats, recs, {})
    ## Count of cars grouped by date
    2016-12-01 2
    2016-12-02 200
    2016-12-03 300
    >>> state['total-cars']
    502
    >>> from pprint import pprint
    >>> pprint(state['ranks'])
    [(300, '2016-12-03T00:00:00'),
     (200, '2016-12-02T00:00:00'),
     (1, '2016-12-01T07:30:00')]
    """

    # This special reducer needs the END_OF_INPUT...
    stream_group_by_date(state, record)

    # ... these don't
    (datetime, cars) = record
    if datetime != END_OF_INPUT[0]:
        # Sum
        state['total-cars'] = state.get('total-cars', 0) + cars

        # Top 3 cars
        rank(3, state, (cars, datetime))

        # Least of 3 consecutive records
        min_sum_in_window(3, state, (cars, datetime))

    return state


def print_stats(state):
    "Takes the result of reducing accumulate_stats"
    print('## Total cars = ', state['total-cars'])

    print('## Top 3 half hours\n' +
          '\n'.join(map(lambda x: '{1} {0}'.format(*x), state['ranks'])))

    print('## 1.5 hour period with ​least​ cars = {0} cars [{1} .. {2}]'
          .format(*state['min_window']))


def streamfile(filename):
    """
    Generator that streams a file line by line
    >>> stream = streamfile("data.file")
    >>> next(stream)
    '2016-12-01T05:00:00 5\\n'
    >>> next(stream)
    '2016-12-01T05:30:00 12\\n'
    """
    for line in open(filename):
        yield line


def parse(line):
    """
    Parses a data line into the tuple (datetime:string, count:int)

    >>> parse('2016-12-01T15:30:00 11')
    ('2016-12-01T15:30:00', 11)
    """
    (datetime, cars) = line.strip().split(' ')
    return (datetime, int(cars))


def run(filename):
    """
    Runs the entire report

    >>> run("data.file")
    ## Count of cars grouped by date
    2016-12-01 179
    2016-12-05 81
    2016-12-08 134
    2016-12-09 4
    ## Total cars =  398
    ## Top 3 half hours
    2016-12-01T07:30:00 46
    2016-12-01T08:00:00 42
    2016-12-08T18:00:00 33
    ## 1.5 hour period with ​least​ cars = 20 cars [2016-12-01T15:00:00 .. 2016-12-01T23:30:00]
    """
    lines = streamfile(filename)
    records = map(parse, lines)
    # Add a special signal for the grouping!
    records = chain(records, [END_OF_INPUT])
    state = reduce(accumulate_stats, records, {})
    print_stats(state)


if __name__ == '__main__':
    run(argv[1])
