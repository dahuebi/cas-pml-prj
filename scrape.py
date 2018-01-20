#!/usr/bin/env python3

import sys
import os
import logging
import codecs
import json
import datetime
import itertools
import csv

# windows does not know os.EX_OK
if os.name == "nt":
  os.EX_OK = 0

__file__ = os.path.abspath(__file__)
__scriptdir__ = os.path.dirname(__file__)

sys.path.insert(0, os.path.join(__scriptdir__, "coinmarketcap-scraper"))
sys.path.insert(0, os.path.join(__scriptdir__, "coinmarketcap-history"))

import coinmarketcap
import coinmarketcap_usd_history

################################################################################
# coinmarketcap-scraper
def scrapeCoinList():
    """Scrape coin list."""
    coinmarketcap.lastReqTime = None
    html = coinmarketcap.requestList('coins', 'all')
    data = coinmarketcap.parseList(html, 'currencies')
    return data


def scrapeTokenList():
    """Scrape token list."""
    coinmarketcap.lastReqTime = None
    html = coinmarketcap.requestList('tokens', 'all')
    data = coinmarketcap.parseList(html, 'assets')
    return data

################################################################################
def downloadCurrency(currency, startDate, endDate):
  print(currency, startDate, endDate)
  html = coinmarketcap_usd_history.download_data(currency, startDate, endDate)
  header, rows = coinmarketcap_usd_history.extract_data(html) 
  return header, rows

################################################################################
class Scrape:
  TOKENS = "tokens"
  COINS = "coins"

  def __init__(self, datadir="data"):
    self.datadir = datadir
    self.coins = []
    self.tokens = []
    if not os.path.exists(self.datadir):
      os.makedirs(self.datadir)

  def run(self):
    self.scrapeCurrencies()    
    self.updateData()
    return os.EX_OK

  def getPath(self, *args):
    return os.path.join(self.datadir, *args)

  def saveCurrencies(self, currencies, filename):
    path = self.getPath(filename)
    with codecs.open(path, "w", encoding="UTF-8") as fp:
      fp.write(json.dumps(currencies, indent=4))

  def loadCurrencies(self, filename):
    path = self.getPath(filename)
    currencies = []
    if not os.path.exists(path):
      return currencies
    with codecs.open(path, "r", encoding="UTF-8") as fp:
      try:
        currencies = json.loads(fp.read())
      except json.decoder.JSONDecodeError:
        pass
    return currencies

  def scrapeCurrencies(self):
    currencies = []
    for func, path in ((scrapeCoinList, "{}.txt".format(self.COINS)),
        (scrapeTokenList, "{}.txt".format(self.TOKENS))):
      c = self.loadCurrencies(path)
      if not c:
        c = func()
        self.saveCurrencies(c, path)
      currencies.append(c)

    self.coins = currencies[0]
    self.tokens = currencies[1]

  def downloadCurrency(self, currency, directory):
    parseDate = lambda s: datetime.datetime.strptime(s, "%Y-%m-%d")
    path = self.getPath(directory, "{}.csv".format(currency))
    data = []
    startDate = parseDate("2001-01-01")
    endDate = datetime.datetime.utcnow()
    endDate = endDate.replace(hour=0, minute=0, second=0, microsecond=0)
    if os.path.exists(path):
      with codecs.open(path, "r", encoding="UTF-8") as fp:
        reader = csv.reader(fp)
        try:
          # skip header
          next(reader)
        except StopIteration:
          pass
        for row in reader:
          startDate = max(startDate, parseDate(row[0]))
          data.append(row)
    # increment one day
    startDate += datetime.timedelta(days=1)
    if startDate >= endDate:
      return
    header, rows = downloadCurrency(currency,
      startDate.strftime("%Y%m%d"),
      endDate.strftime("%Y%m%d"))
    if not rows:
      return
    header = header[:-1]
    for row in rows:
      row = row[:-1] # remove Average
      # parse date from module
      row[0] = datetime.datetime.strptime(row[0], "%b %d %Y").strftime("%Y-%m-%d")
      data.append(row)
    data = sorted(data, key=lambda row: row[0], reverse=True)
    with codecs.open(path, "w", encoding="UTF-8") as fp:
      writer = csv.writer(fp, quoting=csv.QUOTE_NONE)
      [writer.writerow(row) for row in itertools.chain([header], data)]
  
  def updateData(self):
    for currencies, directory in ((self.coins, self.COINS),
      (self.tokens, self.TOKENS)):
      d = self.getPath(directory)
      if not os.path.exists(d):
        os.makedirs(d)
      [self.downloadCurrency(c["slug"], directory) for c in currencies]

  @classmethod
  def main(cls, argv=sys.argv[1:]):
    logging.basicConfig(
      level=logging.ERROR,
      format='%(asctime)s %(levelname)s: %(message)s',
      datefmt='%m/%d/%Y %I:%M:%S %p')

    scrape = cls() 
    return scrape.run()

if __name__ == '__main__':
  sys.exit(Scrape.main())
