from collections import namedtuple
from itertools import groupby
from functools import reduce, partial

Rec = namedtuple('Rec', ['datetime', 'cars'])


def readfile(filename):
    for line in open(filename):
        yield line

def parse(line):
    """
    Parses a data line into the tuple (datetime:string, count:int)

    >>> parse('2016-12-01T15:30:00 11')
    Rec(datetime='2016-12-01T15:30:00', cars=11)
    """
    (datetime, cars) = line.strip().split(' ')
    return Rec(datetime, int(cars))


def pickDate(rec):
    return rec.datetime.split('T')[0]

def pickCars(rec):
    return rec.cars

def reduce_to_sum_cars(accum, rec):
    return accum + rec.cars

def rank(max_ranks, accum, record):
    """
    A reducer, that ranks all your records by `max_ranks` positions.

    Single value:
    >>> rank(3, {}, Rec('first', 1))
    [Rec(datetime='first', cars=1)]

    Top 3 in the right order
    >>> recs = [Rec('1st', 1), Rec('2nd', 2), Rec('3rd', 3)]
    >>> reduce(partial(rank, 3), recs, {})
    [Rec(datetime='3rd', cars=3), Rec(datetime='2nd', cars=2), Rec(datetime='1st', cars=1)]

    Insertion and clamping:
    >>> recs = [Rec('1st', 10), Rec('2nd', 20), Rec('3rd', 30), Rec('4th', 25)]
    >>> reduce(partial(rank, 3), recs, {})
    [Rec(datetime='3rd', cars=30), Rec(datetime='4th', cars=25), Rec(datetime='2nd', cars=20)]
    """
    if len(accum) == 0:
        return [record] 

    # Search for an insertion point
    for index, leader in enumerate(accum):
        if record.cars > leader.cars:
            accum.insert(index, record)
            if len(accum) > max_ranks:
                accum.pop()
            return accum

    # No change
    return accum


if __name__ == '__main__':
    lines = readfile('data.file')
    records = map(parse, lines)
    dateGroups = groupby(records, key=pickDate)

    total_cars = 0
    ranks = []
    rank3 = partial(rank, 3)

    for v, group in dateGroups:
        group = list(group)
        print(v, sum(map(pickCars, group)))
        total_cars = reduce(reduce_to_sum_cars, group, total_cars)
        ranks = reduce(rank3, group, ranks)
    
    print('total cars=', total_cars)
    print('top 3 =', ranks)
