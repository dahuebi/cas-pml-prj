#!/usr/bin/env python3

import os
import pandas as pd

__file__ = os.path.abspath(__file__)
__scriptdir__ = os.path.dirname(__file__)

def fillMissingSamples(df):
    cnt = 0
    grouped = df.groupby(["slug"], sort=False)
    groups = []
    for slug, group in grouped:
        assert(len(group.index) == len(group.index.unique()))
        name = group.name.unique()[0]
        index = pd.to_datetime(group["date"], format="%Y%m%d")
        group.set_index(index, inplace=True)
        group = group.drop("date", axis=1)
        head = group.iloc[0].head(1).name
        tail = group.iloc[-1].head(1).name
        newIndex = pd.date_range(head, tail)
        group = group.reindex(newIndex)
        date = group.index.strftime("%Y%m%d")
        group.insert(0, "date", date)
        if group.isnull().values.any():
            group.slug = slug
            group.name = name
            cnt += len(group[group.isnull().any(axis=1)])
            group.interpolate(inplace=True)

        assert(not group.isnull().values.any())
        group = group.reset_index(drop=True)
        groups.append(group)

    print("Samples filled: {}".format(cnt))
    return pd.concat(groups)

def filterMinSamples(df, minSamples):
    grouped = df.groupby(["slug"]).size()
    sampleFilter = grouped[grouped >= minSamples]
    return df[df.slug.isin(sampleFilter.index)]

def filterMinVolumeAndMinMarketCap(df, minVolume, minMarketCap):
    names = df[(df.volume >= minVolume) & \
            (df["marketcap"] >= minMarketCap)].slug.unique()
    return df[df.slug.isin(names)]

def load(basedir=__scriptdir__,
        minSamples=365,
        minVolume=1000*1000,
        minMarketCap=1000*1000,
        fillMissingDates=True):
    coinmarketcap = os.path.join(basedir, "coinmarketcap.csv")
    df = pd.read_csv(coinmarketcap)
    df = filterMinSamples(df, minSamples)
    df = filterMinVolumeAndMinMarketCap(df, minVolume, minMarketCap)

    # fill missing values
    if fillMissingDates:
        df = fillMissingSamples(df)

    print("Loaded {} currencies, {} samples.".format(
        len(df.slug.unique()), len(df)))
    return df

if __name__ == "__main__":
    load()

