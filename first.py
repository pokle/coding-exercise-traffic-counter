

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


def accumulate(state, line):
    """
    Accumulates state, line by line

    Single day:
    >>> s = {}
    >>> accumulate(s, "2016-12-01T07:30:00 46")
    >>> accumulate(s, "2016-12-01T08:00:00 42")
    >>> s['total-cars']
    88

    Multiple days:
    >>> s = {}
    >>> accumulate(s, "2016-12-01T07:30:00 1")
    >>> accumulate(s, "2016-12-01T08:00:00 1")
    >>> accumulate(s, "2016-12-02T00:00:00 200")
    2016-12-01   2
    >>> accumulate(s, "2016-12-03T00:00:00 300")
    2016-12-02   200
    >>> accumulate(s, "0 0")  # Finish
    2016-12-03   300
    >>> s['total-cars']
    502
    """

    # Parse
    (datetime, cars) = line.split(' ')
    cars = int(cars)
    date = datetime.split('T')[0]

    # Sum
    state['total-cars'] = state.get('total-cars', 0) + cars

    # Group by day
    if 'last-date' not in state:
        state['last-date'] = date
        state['date-cars'] = cars
    elif state['last-date'] != date:
        print(state['last-date'], ' ', state['date-cars'])
        state['last-date'] = date
        state['date-cars'] = cars
    else:
        state['date-cars'] += cars

    # Top 3 records
    topN(3, state, cars, datetime)

def run(filename):
    """
    >>> run("data.file")
    2016-12-01   179
    2016-12-05   81
    2016-12-08   134
    2016-12-09   4
    ## Total cars =  398
    ## Top 3 cars =  [(46, '2016-12-01T07:30:00'), (42, '2016-12-01T08:00:00'), (33, '2016-12-08T18:00:00')]
    """
    with open(filename) as f:
        state = {}
        for line in f:
            accumulate(state, line)
        accumulate(state, "0 0")  # Signal end
        print('## Total cars = ', state['total-cars'])
        print('## Top 3 cars = ', state['leaders'])


if __name__ == '__main__':
    run("data.file")
