from sys import argv
from collections import namedtuple
from itertools import groupby
from functools import reduce, partial

# All our reducers work on a labeled values
LV = namedtuple('LV', ['label', 'value'])


def readfile(filename):
    for line in open(filename):
        yield line

def parse(line):
    """
    Parses a data line into an LV

    >>> parse('2016-12-01T15:30:00 11')
    LV(label='2016-12-01T15:30:00', value=11)
    """
    (label, value) = line.strip().split(' ')
    return LV(label, int(value))


def pickDate(lv):
    return lv.label.split('T')[0]

def pickCars(lv):
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
        return [lv] # Straight to the top

    # Search for an insertion point
    for index, leader in enumerate(accum):
        if lv.value > leader.value:
            accum.insert(index, lv)
            if len(accum) > max_ranks:
                accum.pop()
            return accum

    # No change
    return accum


def run(file):
    lines = readfile(file)
    lvs = map(parse, lines)
    dateGroups = groupby(lvs, key=pickDate)

    total_value = 0
    ranks = []
    rank3 = partial(rank, 3)

    for v, group in dateGroups:
        group = list(group)
        groupTot = sum(map(pickCars, group))
        print(v, groupTot)
        total_value += groupTot
        ranks = reduce(rank3, group, ranks)
    
    print('total value=', total_value)
    print('top 3 =', ranks)

if __name__ == '__main__':
    run(argv[1])
