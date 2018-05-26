from sys import argv
from collections import deque


def min_window(windowSize, state, contender, label):
    """
    Finds the minimum sliding window.

    Result in state['min_window'] is [ <VALUE>, <START_LABEL>, <END_LABEL> ]

    Not enough data, so just pick the smallest range
    >>> state = {}
    >>> min_window(3, state, 5, 'first')
    >>> state
    {'window': deque([(5, 'first')]), 'min_window': [5, 'first', 'first']}

    Fill up window
    >>> state = {}
    >>> min_window(3, state, 40, '1st')
    >>> min_window(3, state, 42, '2nd')
    >>> min_window(3, state, 35, '3rd')
    >>> from pprint import pprint
    >>> pprint(state)
    {'min_window': [117, '1st', '3rd'],
     'window': deque([(40, '1st'), (42, '2nd'), (35, '3rd')])}

    Add one more smaller than the min_window to move the window
    >>> min_window(3, state, 0, '4th')
    >>> state
    {'window': deque([(42, '2nd'), (35, '3rd'), (0, '4th')]), 'min_window': [77, '2nd', '4th']}

    Add one larger than the min_window so that the min doesn't change.
    >>> min_window(3, state, 1000, '5th')
    >>> pprint(state)
    {'min_window': [77, '2nd', '4th'],
     'window': deque([(35, '3rd'), (0, '4th'), (1000, '5th')])}
    """
    
    if 'window' not in state:
        state['window'] = deque([(contender, label)])
        state['min_window'] = [contender, label, label]
        return

    min_window = state['min_window']
    window = state['window']

    if len(window) < windowSize:
        # Keep accumulating until we have enough data
        window.append((contender, label))
        min_window[0] += contender
        min_window[2] = label
        return

    # Slide window
    window.popleft()
    window.append((contender, label))

    window_sum = sum(map(lambda t: t[0], window))
    if min_window[0] > window_sum:
        min_window[0] = window_sum
        min_window[1] = window[0][1]
        min_window[2] = window[-1][1]


def topN(n, state, contender, label):
    """
    Single value:
    >>> state = {}
    >>> g = topN(3, state, 1, 'first')
    >>> state
    {'leaders': [(1, 'first')]}

    Top 3 in the right order
    >>> state = {}
    >>> g = topN(3, state, 1, '1st')
    >>> g = topN(3, state, 2, '2nd')
    >>> g = topN(3, state, 3, '3rd')
    >>> state
    {'leaders': [(3, '3rd'), (2, '2nd'), (1, '1st')]}

    Insertion and clamping:
    >>> state = {}
    >>> g = topN(3, state, 10, '1st')
    >>> g = topN(3, state, 20, '2nd')
    >>> g = topN(3, state, 30, '3rd')
    >>> g = topN(3, state, 25, '4th')
    >>> state
    {'leaders': [(30, '3rd'), (25, '4th'), (20, '2nd')]}

    """
    if 'leaders' not in state:
        state['leaders'] = [(contender, label)]
        return

    leaders = state['leaders']

    for index, leader in enumerate(leaders):
        if contender > leader[0]:
            leaders.insert(index, (contender, label))
            if len(leaders) > n:
                leaders.pop()
            return


END_OF_INPUT = "GOODBYE 0"


def accumulate(state, line):
    """
    Accumulates state, line by line.

    Single day:
    >>> s = {}
    >>> accumulate(s, "2016-12-01T07:30:00 46")
    ## Count of cars grouped by date
    >>> accumulate(s, "2016-12-01T08:00:00 42")
    >>> accumulate(s, END_OF_INPUT)
    2016-12-01   88
    >>> s['total-cars']
    88

    Multiple days:
    >>> s = {}
    >>> accumulate(s, "2016-12-01T07:30:00 1")
    ## Count of cars grouped by date
    >>> accumulate(s, "2016-12-01T08:00:00 1")
    >>> accumulate(s, "2016-12-02T00:00:00 200")
    2016-12-01   2
    >>> accumulate(s, "2016-12-03T00:00:00 300")
    2016-12-02   200
    >>> accumulate(s, END_OF_INPUT)
    2016-12-03   300
    >>> s['total-cars']
    502
    >>> from pprint import pprint
    >>> pprint(s['leaders'])
    [(300, '2016-12-03T00:00:00'),
     (200, '2016-12-02T00:00:00'),
     (1, '2016-12-01T07:30:00')]
    """

    # Parse
    (datetime, cars) = line.strip().split(' ')
    cars = int(cars)
    date = datetime.split('T')[0]

    # Group by day
    if 'last-date' not in state:
        print('## Count of cars grouped by date')
        state['last-date'] = date
        state['date-cars'] = cars
    elif state['last-date'] != date:
        print(state['last-date'], ' ', state['date-cars'])
        state['last-date'] = date
        state['date-cars'] = cars
    else:
        state['date-cars'] += cars

    if line != END_OF_INPUT:
        # Sum
        state['total-cars'] = state.get('total-cars', 0) + cars

        # Top 3 records
        topN(3, state, cars, datetime)

        # Least of 3 consequitive records
        min_window(3, state, cars, datetime)


def final_report(state):
    print('## Total cars = ', state['total-cars'])
    print('## Top 3 cars\n' +
          '\n'.join(map(lambda x: f'{x[1]} {x[0]}',  state['leaders'])))
    print('## Least window = ', state['min_window'])


def run(filename):
    """
    >>> run("data.file")
    ## Count of cars grouped by date
    2016-12-01   179
    2016-12-05   81
    2016-12-08   134
    2016-12-09   4
    ## Total cars =  398
    ## Top 3 cars
    2016-12-01T07:30:00 46
    2016-12-01T08:00:00 42
    2016-12-08T18:00:00 33
    ## Least window =  [20, '2016-12-01T15:00:00', '2016-12-01T23:30:00']
    """
    with open(filename) as f:
        state = {}
        for line in f:
            accumulate(state, line)
        accumulate(state, END_OF_INPUT)
        final_report(state)


if __name__ == '__main__':
    run(argv[1])
