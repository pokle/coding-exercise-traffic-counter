"""
Traffic report coding test
- Tushar Pokle
"""

import sys
import io
from collections import namedtuple, deque
from itertools import groupby
from functools import reduce, partial

# *Jargon*
# All our reducers work on a labeled values
# Here, the datetime or date is the label
# and the number of cars is the value
LV = namedtuple('LV', ['label', 'value'])


def streamfile(filename=None):
    """
    Generator that streams a file line by line
    >>> stream = streamfile("data.file")
    >>> next(stream)
    '2016-12-01T05:00:00 5\\n'
    >>> next(stream)
    '2016-12-01T05:30:00 12\\n'
    """
    if filename:
        for line in open(filename):
            yield line
    else:
        for line in sys.stdin:
            yield line


def parse(line):
    """
    Parses a data line into an LV

    >>> parse('2016-12-01T15:30:00 11')
    LV(label='2016-12-01T15:30:00', value=11)
    """
    (label, value) = line.strip().split(' ')
    return LV(label, int(value))


def pick_date(lv):
    """
    When an LV is an ISO8601 string, this gives you the date part:
    >>> pick_date(LV("2016-12-01T15:30:00", 11))
    '2016-12-01'
    """
    return lv.label.split('T')[0]


def pick_value(lv):
    "Just the value part of a labeled value"
    return lv.value


def rank(max_ranks, accum, lv):
    """
    A reducer, that ranks all your LVs by `max_ranks` positions.

    Single value:
    >>> rank(3, {}, LV('first', 1))
    [LV(label='first', value=1)]

    Top 3 in the right order
    >>> lvs = [LV('1st', 1), LV('2nd', 2), LV('3rd', 3)]
    >>> reduce(partial(rank, 3), lvs, {})
    [LV(label='3rd', value=3), LV(label='2nd', value=2), LV(label='1st', value=1)]

    Insertion and clamping:
    >>> lvs = [LV('1st', 10), LV('2nd', 20), LV('3rd', 30), LV('4th', 25)]
    >>> reduce(partial(rank, 3), lvs, {})
    [LV(label='3rd', value=30), LV(label='4th', value=25), LV(label='2nd', value=20)]
    """
    if len(accum) == 0:
        return [lv]  # Straight to the top

    # Search for an insertion point
    for index, leader in enumerate(accum):
        if lv.value > leader.value:
            accum.insert(index, lv)
            if len(accum) > max_ranks:
                accum.pop()
            return accum

    # No change
    return accum


RANK3 = partial(rank, 3)


def min_sum_in_window(window_size, accum, lv):
    """
    A reducer that finds a sliding window with the minimum sum

    Result is a dict with { min_window: [<VALUE>, <START_LABEL>, <END_LABEL>] }

    Not enough data, so just pick the smallest range:
    >>> min_sum_in_window(3, {}, LV('first', 5))
    {'window': deque([LV(label='first', value=5)]), 'min_window': [5, 'first', 'first']}

    Fill up window:
    >>> recs = [LV('1st', 40), LV('2nd', 42), LV('3rd', 35)]
    >>> accum = reduce(partial(min_sum_in_window, 3), recs, {})
    >>> accum
    {'window': deque([LV(label='1st', value=40), LV(label='2nd', value=42), LV(label='3rd', value=35)]), 'min_window': [117, '1st', '3rd']}

    Add one more smaller than the min_window to move the window
    >>> accum = min_sum_in_window(3, accum, LV('4th', 0))
    >>> accum
    {'window': deque([LV(label='2nd', value=42), LV(label='3rd', value=35), LV(label='4th', value=0)]), 'min_window': [77, '2nd', '4th']}

    Add one larger than the min_window so that the min doesn't change.
    >>> min_sum_in_window(3, accum, LV('5th', 1000))
    {'window': deque([LV(label='3rd', value=35), LV(label='4th', value=0), LV(label='5th', value=1000)]), 'min_window': [77, '2nd', '4th']}
    """

    # First time around
    if 'window' not in accum:
        accum['window'] = deque([lv])
        accum['min_window'] = [lv.value, lv.label, lv.label]
        return accum

    min_window = accum['min_window']
    window = accum['window']

    if len(window) < window_size:
        # Keep accumulating until we have enough data
        window.append(lv)
        min_window[0] += lv.value
        min_window[2] = lv.label
        return accum

    # Slide window
    window.popleft()
    window.append(lv)

    window_sum = sum(map(pick_value, window))
    if min_window[0] > window_sum:
        min_window[0] = window_sum       # sum(start..end)
        min_window[1] = window[0].label  # start
        min_window[2] = window[-1].label  # end

    return accum


MIN_SUM_IN_WINDOW_SIZE_3 = partial(min_sum_in_window, 3)


def run(file=None):
    """
    Generates the report

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

    lines = streamfile(file)
    labeled_values = map(parse, lines)

    # We accumulate stats for the entire file in these vars
    total_cars = 0
    ranks = []
    min_sum = {}

    print('## Count of cars grouped by date')

    date_groups = groupby(labeled_values, key=pick_date)
    for group_label, group in date_groups:
        # We stream the groups to stdout to keep
        # memory usage constant
        group = list(group)
        group_tot = sum(map(pick_value, group))
        print(group_label, group_tot)

        # These reducers have to keep accumulating
        # stats till the end
        total_cars += group_tot
        ranks = reduce(RANK3, group, ranks)
        min_sum = reduce(MIN_SUM_IN_WINDOW_SIZE_3, group, min_sum)

    # Finally, print the report
    print('## Total cars = ', total_cars)
    print('## Top 3 half hours\n' +
          '\n'.join(map(lambda x: '{0} {1}'.format(*x), ranks)))
    print('## 1.5 hour period with least cars = {0} cars [{1} .. {2}]'
          .format(*min_sum['min_window']))


if __name__ == '__main__':
    if len(sys.argv) == 2:
        run(sys.argv[1])
    else:
        run()
