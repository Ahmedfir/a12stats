
# https://gist.github.com/timm/5630491
import math
from typing import List


# @see https://pypi.org/project/cliffs-delta/
# @see https://stats.stackexchange.com/a/194069
def c_delta(x1: List[float], x2: List[float]):
    from cliffs_delta import cliffs_delta
    return cliffs_delta(x1, x2)


def cliffs_delta_vd_a(x1: List[float], x2: List[float], reverse=True):
    "how often is x in x1 more than y in x2?"
    cd, res = c_delta(x2, x1) if reverse else c_delta(x1, x2)
    return ((cd + 1.0) / 2.0), res


def a12(lst1, lst2, rev=True):
    "how often is x in lst1 more than y in lst2?"
    more = same = 0.0
    for x in lst1:
        if math.isnan(x):
            x = 0.0
        for y in lst2:
            if math.isnan(y):
                y = 0.0
            if x == y:
                same += 1
            elif not rev and x > y:
                more += 1
            elif rev and x < y:
                more += 1
    return (more + 0.5 * same) / (len(lst1) * len(lst2))


def a12_paired(lst1, lst2, rev=True):
    "how often is x in lst1 more than y in lst2?"
    more = same = 0.0
    assert len(lst1) == len(lst2)
    for index, x in enumerate(lst1):
        y = lst2[index]

        if math.isnan(x):
            x = 0.0
        if math.isnan(y):
            y = 0.0
        if x == y:
            same += 1
        elif not rev and x > y:
            more += 1
        elif rev and x < y:
            more += 1
    return (more + 0.5 * same) / len(lst1)


def a12Paired(df1, df2, column, group_by_column, rev=True):
    more = same = 0.0
    assert len(df1) == len(df2)
    for index, row in df1.iterrows():
        x = row[column]
        df2SameBug = df2[df2[group_by_column] == row[group_by_column]]
        assert len(df2SameBug) == 1
        y = df2SameBug.iloc[0][column]

        if math.isnan(x):
            x = 0.0
        if math.isnan(y):
            y = 0.0

        if x == y:
            same += 1
        elif rev and x > y:
            more += 1
        elif not rev and x < y:
            more += 1
    return (more + 0.5 * same) / len(df1)