#!/usr/bin/env python3

import redis
import json
import pandas as pd


def fetch_key_value_counts():
    """
    Fetch key and value counts from Redis and save them to JSON files.
    """
    red = redis.Redis(host="geth", port=6379, db=3)

    keys = [k.decode() for k in red.keys()]

    ks = [k for k in keys if k.startswith("k-")]
    key_counts = []
    for k in ks:
        _, prefix, length = k.split("-")
        count = int(red.get(k).decode())
        key_counts.append({"prefix": prefix, "length": int(length), "count": count})

    sorted(key_counts, key=lambda x: x["count"])
    with open("key_counts.json", "w") as f:
        json.dump(key_counts, f, indent=2)

    vs = [k for k in keys if k.startswith("v-")]
    val_counts = []
    for v in vs:
        _, length = v.split("-")
        count = int(red.get(v).decode())
        val_counts.append({"length": int(length), "count": count})
    sorted(val_counts, key=lambda x: x["count"])
    with open("val_counts.json", "w") as f:
        json.dump(val_counts, f, indent=2)


def analyze_key_count():
    df = pd.read_json("key_counts.json")
    df = (
        df.groupby(["length"])
        .agg({"count": "sum", "prefix": lambda x: ",".join(x)})
        .reset_index()
    )
    total = df["count"].sum()
    df["percentage"] = round(df["count"] * 100 / total, 2)
    df.sort_values(by="count", ascending=False, inplace=True)

    # In [34]: df[df.percentage > 1]
    # Out[34]:
    #    prefix  length       count  percentage
    # 0      6c      33  2718121081       42.42
    # 1      6f      65  1255816825       19.60
    # 2      4f      38   405770871        6.33
    # 3      4f      37   398349989        6.22
    # 4      4f      39   353845432        5.52
    # 5      61      33   279625041        4.36
    # 6      4f      36   210959913        3.29
    # 7      41       8   173793595        2.71
    # 8      41       9   172056223        2.69
    # 9      4f      40   158045169        2.47
    # 10     4f      35    83748492        1.31


def step_of(x):
    """
    Calculate the step of a number.
    """
    if x < 16:
        return "<16"
    elif x < 64:
        return "<64"
    elif x < 128:
        return "<128"
    elif x < 1024:
        return "<1kb"
    elif x < 4096:
        return "<4kb"
    elif x < 8192:
        return "<8kb"
    elif x < 1024 * 1024:
        return "<1mb"
    else:
        return ">1mb"


def analyze_val_count():
    df = pd.read_json("val_counts.json")
    total = df["count"].sum()
    df["percentage"] = round(df["count"] * 100 / total, 2)
    df.sort_values(by="count", ascending=False, inplace=True)

    # In [56]: df[df.percentage > 1]
    # Out[56]:
    #     length       count  percentage
    # 0        3  2000247283       31.22
    # 1        4   868115807       13.55
    # 2        1   303561017        4.74
    # 3       33   291424174        4.55
    # 4       83   287303002        4.48
    # 5       21   179086721        2.79
    # 6        5   175924601        2.75
    # 7      104   131040556        2.05
    # 8       37   109775951        1.71
    # 9       34   102075286        1.59
    # 10      12    89850480        1.40
    # 11     115    89159063        1.39
    # 12      10    80660648        1.26
    # 13     111    75178397        1.17
    # 14      54    73902361        1.15
    # 15      67    71923024        1.12
    # 16      11    71310357        1.11
    # 17       9    69074188        1.08
    # 18      32    68287146        1.07

    # the largest value
    # df.sort_values(by='length', ascending=False).reset_index().drop(columns=['index']).head(10)
    # Out[65]:
    #       length  count  percentage
    # 0  430257431      1         0.0
    # 1    7930079      1         0.0
    # 2     710860      1         0.0
    # 3     213307      1         0.0
    # 4     210228      1         0.0
    # 5     197453      1         0.0
    # 6     193170      1         0.0
    # 7     190263      1         0.0
    # 8     187551      1         0.0
    # 9     185880      1         0.0

    df["step"] = df["length"].apply(step_of)
    df.groupby(["step"]).agg({"count": "sum", "percentage": "sum"}).reset_index()
    # Out[78]:
    #    step       count  percentage cum-percentage
    # 0   <16  3762937229       58.74   58.74
    # 1   <64  1623984858       25.34   84.08
    # 2  <128   855029504       13.33   97.41
    # 3  <1kb   164608814        2.51
    # 4  <1mb      466160        0.00
    # 5  <4kb      351524        0.00
    # 6  <8kb      391346        0.00
    # 7  >1mb           2        0.00


def main():
    fetch_key_value_counts()


if __name__ == "__main__":
    main()
