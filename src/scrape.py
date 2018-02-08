#!/usr/bin/env python3

import os
import sys
import asyncio
import datetime
import re
import json
import codecs
import io
import concurrent.futures
import csv
from pprint import pprint

import requests
import lxml.html

string2datetime = lambda s: datetime.datetime.strptime(s, "%Y%m%d")
datetime2string = lambda dt: dt.strftime("%Y%m%d")

# coinmarketcap-scraper
def parseList(html, type):
    """Parse the information returned by requestList for view 'all'."""
    data = []
    docRoot = lxml.html.fromstring(html)
    rows = docRoot.cssselect(
        "table#{0}-all > tbody > tr".format(type))

    for row in rows:
        datum = {}
        fields = row.cssselect("td")

        # Name and slug
        nameField = fields[1].cssselect("a")[0]
        datum['name'] = nameField.text_content().strip()
        datum['slug'] = nameField.attrib['href'].replace(
            '/currencies/', '').replace('/', '').strip()

        # Symbol
        datum['symbol'] = fields[2].text_content().strip()

        # Explorer link
        supplyFieldPossible = fields[5].cssselect("a")
        if len(supplyFieldPossible) > 0:
            datum['explorer_link'] = supplyFieldPossible[0].attrib['href']
        else:
            datum['explorer_link'] = ''
        data.append(datum)

    return data

# coinmarketcap-history
def extract_data(html):
  """
  Extract the price history from the HTML.

  The CoinMarketCap historical data page has just one HTML table.  This table contains the data we want.
  It's got one header row with the column names.

  We need to derive the "average" price for the provided data.
  """

  head = re.search(r'<thead>(.*)</thead>', html, re.DOTALL).group(1)
  header = re.findall(r'<th .*>([\w ]+)</th>', head)

  body = re.search(r'<tbody>(.*)</tbody>', html, re.DOTALL).group(1)
  raw_rows = re.findall(r'<tr[^>]*>' + r'\s*<td[^>]*>([^<]+)</td>'*7 + r'\s*</tr>', body)

  # strip commas
  rows = []
  for row in raw_rows:
    row = [ re.sub(",", "", field) for field in row ]
    row = [ re.sub("-", "0", field) for field in row ]
    # convert date
    row[0]= datetime.datetime.strptime(row[0], "%b %d %Y").strftime("%Y%m%d")
    rows.append(row)

  return header, rows

__file__ = os.path.abspath(__file__)
__scriptdir__ = os.path.dirname(__file__)

async def main(urls, responses):
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                None, 
                requests.get, 
                url,
            )
            for url in urls
        ]
        for response in await asyncio.gather(*futures):
            responses.append(response)

def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)

URL_COINMARKETCAP = "https://coinmarketcap.com"

# https://coinmarketcap.com/currencies/bitcoin/historical-data/?start=20180127&end=20180202
def loadCache(path):
    path = os.path.abspath(path)
    try:
        with codecs.open(path, "r", encoding="UTF8") as fp:
            return fp.read()
    except OSError:
        pass
    return ""

def saveCache(path, content):
    path = os.path.abspath(path)
    mkdir(os.path.dirname(path))
    with codecs.open(path, "w", encoding="UTF8") as fp:
        fp.write(content)

class CoinMarketCap:
    CURRENCY_URL = URL_COINMARKETCAP + "/{}/views/all"
    SLUG_URL = URL_COINMARKETCAP + "/currencies/{}/historical-data/?start={}&end={}"

    def __init__(self, basedir=__scriptdir__, forceUpdate=False):
        self.cache = os.path.join(basedir, "cache")
        self.coinmarketcap = os.path.join(basedir, "coinmarketcap.csv")
        self.coins = []
        self.tokens = []
        self.forceUpdate = forceUpdate

    def verbose(self, *args, **kwargs):
        print(*args, **kwargs)

    def getCoinsAndTokens(self):
        # try to load data from cache
        def decodeJson(rawData):
            try:
                return json.loads(rawData)
            except json.decoder.JSONDecodeError:
                pass
            return []
        def encodeJson(pythonDict):
            return json.dumps(pythonDict, indent=4)

        cacheCoins = os.path.join(self.cache, "coins.json")
        cacheTokens = os.path.join(self.cache, "tokens.json")
        coins, tokens = [], []
        if not self.forceUpdate:
            coins = decodeJson(loadCache(cacheCoins))
            tokens = decodeJson(loadCache(cacheTokens))
        if coins and tokens:
            self.verbose("Cached: Coins: {}, Tokens: {}".format(len(coins), len(tokens)))
            return coins, tokens

        # load from web
        loop = asyncio.get_event_loop()

        currencyUrls = [self.CURRENCY_URL.format(type) for type in ["coins", "tokens"]]
        responses = []
        loop.run_until_complete(main(currencyUrls, responses))

        coins = parseList(responses[0].content, "currencies")
        tokens = parseList(responses[1].content, "assets")

        saveCache(cacheCoins, encodeJson(coins))
        saveCache(cacheTokens, encodeJson(tokens))
        self.verbose("Coins: {}, Tokens: {}".format(len(coins), len(tokens)))
        return coins, tokens

    def genCurrencySlugUrl(self, slug,
            start=None, end=None):
        start = start or string2datetime("20100101")
        end = end or datetime.datetime.utcnow() + datetime.timedelta(days=1)
        return self.SLUG_URL.format(slug,
                datetime2string(start),
                datetime2string(end))

    def getSlugCache(self, slug):
        return os.path.join(self.cache, "{}.csv".format(slug))

    def decodeCsv(self, raw):
        reader = csv.reader(raw.splitlines())
        return list(reader)

    def getHistories(self, slugs):
        def decodeCsv(raw):
            return self.decodeCsv(raw)

        def encodeCsv(data):
            fp = io.StringIO()
            writer = csv.writer(fp)
            writer.writerows(data)
            return fp.getvalue()

        def cachePath(slug):
            return self.getSlugCache(slug)

        requests = []
        slugRequestMap = {}
        striptime = lambda dt: datetime.datetime.combine(dt.date(), datetime.time())
        utcnow = striptime(datetime.datetime.utcnow())
        oneDay = datetime.timedelta(days=1)
        for slug in slugs:
            path = cachePath(slug)
            dt = None
            if os.path.exists(path):
                st = os.stat(path)
                dt = datetime.datetime.utcfromtimestamp(st.st_mtime)
                dt = striptime(dt)
            rows = decodeCsv(loadCache(path))
            start = None
            if rows:
                # get latest date
                start = string2datetime(rows[-1][0])
                # add one day
                start += oneDay

            if start:
                if start >= utcnow:
                    continue
                if dt and dt >= utcnow:
                    continue

            url = self.genCurrencySlugUrl(slug, start)
            requests.append(url)
            slugRequestMap[url] = slug

        if not slugRequestMap:
            return 0

        loop = asyncio.get_event_loop()
        responses = []
        while requests:
            self.verbose("\rRequest: {}{}".format(len(requests), " "*20), flush=True, end="")
            loop.run_until_complete(main(requests, responses))
            requests = []
            for r in responses:
                if r.ok: continue
                responses.remove(r)
                requests.append(r.url)
        self.verbose()

        # all responses are OK here!!!
        for r in responses:
            slug = slugRequestMap[r.url]
            _, rawData = extract_data(r.content.decode("UTF8"))
            path = cachePath(slug)
            rows = decodeCsv(loadCache(path))
            rows.extend(rawData)
            rows = sorted(rows, key=lambda r: int(r[0]))
            saveCache(path, encodeCsv(rows))
        return len(responses)

    def run(self):
        self.coins, self.tokens = self.getCoinsAndTokens()
        allCurrencies = self.coins + self.tokens
        slugs = [x["slug"] for x in allCurrencies]
        update = self.getHistories(slugs) != 0

        if not os.path.exists(self.coinmarketcap):
            update = True

        if not update:
            return
        # finally, merge all coins into a single csv
        rowCnt = 0
        with codecs.open(self.coinmarketcap, "w", encoding="UTF8") as fp:
            writer = csv.writer(fp)
            writer.writerow([
                    "date",
                    "slug",
                    "name",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "marketcap"])
            for currency in allCurrencies:
                slug = currency["slug"]
                name = currency["name"]
                path = self.getSlugCache(slug)
                rows = self.decodeCsv(loadCache(path))
                print("\r{}/{}{}".format(slug, len(rows), " "*20), end="", flush=True)
                for row in rows:
                    writer.writerow([row[0]] + [slug, name] + row[1:])
                    rowCnt += 1
        print("\nCurrencies: {}, rows: {}".format(len(allCurrencies), rowCnt))

    @classmethod
    def main(cls, argv=sys.argv[1:]):
        coinMarketCap = cls()
        coinMarketCap.run()
        return 0 

if __name__ == "__main__":
    sys.exit(CoinMarketCap.main())

