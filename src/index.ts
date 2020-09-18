#!/usr/bin/env node
import { strict as assert } from 'assert';
import Axios from 'axios';
import fetchMarkets, { Market } from 'crypto-markets';
import fs from 'fs';
import _ from 'lodash';
import mkdirp from 'mkdirp';
import path from 'path';

const FUNDING_RATES_DIR = 'data';

const SWAP_EXCHANGES = ['Binance', 'BitMEX', 'Huobi', 'OKEx']; // exchanges that has Swap market

interface FundingRate {
  exchange: string;
  pair: string;
  rawPair: string;
  fundingRate: number;
  fundingTime: number;
  fundingTimeStr: string;
}

async function crawlOKExFundingRateCurrent(market: Market): Promise<FundingRate> {
  assert.strictEqual(market.type, 'Swap');

  const response = await Axios.get(
    `https://www.okex.com/api/swap/v3/instruments/${market.id}/funding_time`,
  );
  assert.strictEqual(response.status, 200);

  const obj = response.data as {
    instrument_id: string;
    funding_time: string;
    funding_rate: string;
    estimated_rate: string;
    settlement_time: string;
  };

  const result = {
    exchange: 'OKEx',
    pair: market.pair,
    rawPair: market.id,
    fundingRate: parseFloat(obj.funding_rate),
    fundingTime: new Date(obj.funding_time).getTime(),
    fundingTimeStr: obj.funding_time,
  };

  return result;
}

async function crawlOKExFundingRateHistory(market: Market): Promise<readonly FundingRate[]> {
  assert.strictEqual(market.type, 'Swap');

  const response = await Axios.get(
    `https://www.okex.com/api/swap/v3/instruments/${market.id}/historical_funding_rate`,
  );
  assert.strictEqual(response.status, 200);

  const arr = response.data as ReadonlyArray<{
    instrument_id: string;
    funding_rate: string;
    realized_rate: string;
    interest_rate: string;
    funding_time: string;
  }>;

  const result = arr.map((obj) => ({
    exchange: 'OKEx',
    pair: market.pair,
    rawPair: market.id,
    fundingRate: parseFloat(obj.funding_rate),
    fundingTime: new Date(obj.funding_time).getTime(),
    fundingTimeStr: obj.funding_time,
  }));
  return result;
}

async function crawlOKExFundingRate(
  market: Market,
  startTime: number,
): Promise<readonly FundingRate[]> {
  const result: FundingRate[] = [];

  const fundingRateHistory = await crawlOKExFundingRateHistory(market);
  result.push(...fundingRateHistory);

  const fundingRateCurrent = await crawlOKExFundingRateCurrent(market);
  result.push(fundingRateCurrent);

  return result
    .filter((x) => new Date(x.fundingTime).getTime() >= startTime)
    .sort((x, y) => new Date(x.fundingTime).getTime() - new Date(y.fundingTime).getTime());
}

async function crawlBinanceFundingRateOneshot(
  market: Market,
  startTime: number,
): Promise<readonly FundingRate[]> {
  assert.strictEqual(market.type, 'Swap');

  const response = await Axios.get(
    `https://fapi.binance.com/fapi/v1/fundingRate?symbol=${market.id}&startTime=${startTime}&limit=1000`,
  );
  assert.strictEqual(response.status, 200);

  const arr = response.data as ReadonlyArray<{
    symbol: string;
    fundingTime: number;
    fundingRate: string;
  }>;

  const result = arr.map((obj) => ({
    exchange: 'Binance',
    pair: market.pair,
    rawPair: market.id,
    fundingRate: parseFloat(obj.fundingRate),
    fundingTime: new Date(obj.fundingTime).getTime(),
    fundingTimeStr: new Date(obj.fundingTime).toISOString(),
  }));
  return result;
}

// crawl from startTime until now
async function crawlBinanceFundingRate(
  market: Market,
  startTime: number,
): Promise<readonly FundingRate[]> {
  const result: FundingRate[] = [];

  let fundingRates: readonly FundingRate[] = [];
  do {
    fundingRates = await crawlBinanceFundingRateOneshot(market, startTime); // eslint-disable-line no-await-in-loop
    if (fundingRates.length > 0) {
      result.push(...fundingRates);
      // eslint-disable-next-line no-param-reassign
      startTime = new Date(fundingRates[fundingRates.length - 1].fundingTime).getTime() + 1000; // add 1 second
    }
  } while (fundingRates.length >= 1000); // Binance retuurn 1000 items in maximum

  return result.sort(
    (x, y) => new Date(x.fundingTime).getTime() - new Date(y.fundingTime).getTime(),
  );
}

async function crawlHuobiFundingRateOneshot(
  market: Market,
  pageIndex = 1,
): Promise<readonly FundingRate[]> {
  assert.strictEqual(market.type, 'Swap');

  const response = await Axios.get(
    `https://api.hbdm.com/swap-api/v1/swap_historical_funding_rate?contract_code=${market.id}&page_index=${pageIndex}&page_size=50`,
  );
  assert.strictEqual(response.status, 200);
  assert.strictEqual(response.data.status, 'ok');

  const arr = response.data.data.data as ReadonlyArray<{
    funding_rate: string;
    realized_rate: string;
    funding_time: string;
    contract_code: string;
    symbol: string;
    fee_asset: string;
  }>;

  const result = arr.map((obj) => ({
    exchange: 'Huobi',
    pair: market.pair,
    rawPair: market.id,
    fundingRate: parseFloat(obj.realized_rate),
    fundingTime: new Date(parseFloat(obj.funding_time)).getTime(),
    fundingTimeStr: new Date(parseFloat(obj.funding_time)).toISOString(),
  }));
  return result;
}

// crawl from startTime until now
async function crawlHuobiFundingRate(
  market: Market,
  lastFundingTime = -1,
): Promise<readonly FundingRate[]> {
  const result: FundingRate[] = [];

  let fundingRates: readonly FundingRate[] = [];
  let pageIndex = 1;
  do {
    fundingRates = await crawlHuobiFundingRateOneshot(market, pageIndex); // eslint-disable-line no-await-in-loop
    pageIndex += 1;
    fundingRates = fundingRates.filter(
      (rate) => new Date(rate.fundingTime).getTime() > lastFundingTime, // eslint-disable-line no-loop-func
    );
    if (fundingRates.length > 0) {
      result.push(...fundingRates);
    }
  } while (fundingRates.length >= 50); // Huobi retuurn 50 items per page

  return result.sort(
    (x, y) => new Date(x.fundingTime).getTime() - new Date(y.fundingTime).getTime(),
  );
}

async function crawlBitMEXFundingRateOneshot(
  market: Market,
  startTime = 1568088000000, // '2019-09-10T04:00:00.000Z'
): Promise<readonly FundingRate[]> {
  assert.strictEqual(market.type, 'Swap');

  const response = await Axios.get(
    `https://www.bitmex.com/api/v1/funding?symbol=${
      market.baseId
    }:perpetual&count=500&startTime=${new Date(startTime).toISOString()}`,
  );
  assert.strictEqual(response.status, 200);
  assert.strictEqual(response.statusText, 'OK');

  const arr = response.data as ReadonlyArray<{
    timestamp: string;
    symbol: string;
    fundingInterval: string;
    fundingRate: number;
    fundingRateDaily: number;
  }>;

  const result = arr.map((obj) => ({
    exchange: 'BitMEX',
    pair: market.pair,
    rawPair: market.id,
    fundingRate: obj.fundingRate,
    fundingTime: new Date(obj.timestamp).getTime(),
    fundingTimeStr: obj.timestamp,
  }));
  return result;
}

// crawl from startTime until now
async function crawlBitMEXFundingRate(
  market: Market,
  startTime = 1568088000000, // '2019-09-10T04:00:00.000Z'
): Promise<readonly FundingRate[]> {
  const result: FundingRate[] = [];

  let fundingRates: readonly FundingRate[] = [];
  do {
    fundingRates = await crawlBitMEXFundingRateOneshot(market, startTime); // eslint-disable-line no-await-in-loop
    if (fundingRates.length > 0) {
      result.push(...fundingRates);
      // eslint-disable-next-line no-param-reassign
      startTime = new Date(fundingRates[fundingRates.length - 1].fundingTime).getTime() + 3600000; // add 1 hour
    }
  } while (fundingRates.length >= 500); // BitMEX retuurn 500 items in maximum

  return result.sort(
    (x, y) => new Date(x.fundingTime).getTime() - new Date(y.fundingTime).getTime(),
  );
}

async function crawlFundingRates(market: Market): Promise<void> {
  assert.strictEqual(market.type, 'Swap');

  // get startTime
  if (!fs.existsSync(path.join(FUNDING_RATES_DIR, market.exchange))) {
    mkdirp.sync(path.join(FUNDING_RATES_DIR, market.exchange));
  }
  const fundingRatesFile = path.join(FUNDING_RATES_DIR, market.exchange, `${market.pair}.json`);
  if (fs.existsSync(fundingRatesFile)) {
    try {
      JSON.parse(fs.readFileSync(fundingRatesFile, 'utf8'));
    } catch (err) {
      console.error(fundingRatesFile);
      console.error(err);
    }
  }
  const fundingRatesHistory = fs.existsSync(fundingRatesFile)
    ? (JSON.parse(fs.readFileSync(fundingRatesFile, 'utf8')) as FundingRate[])
    : [];
  const startTime =
    fundingRatesHistory.length <= 0
      ? 1568102400000 // '2019-09-10T08:00:00.000Z'
      : new Date(
          fundingRatesHistory[
            // the latest item of OKEx is not from history
            fundingRatesHistory.length - (market.exchange === 'OKEx' ? 2 : 1)
          ].fundingTime,
        ).getTime() + 1000;

  // eslint-disable-next-line no-shadow
  let crawlFunc: (market: Market, startTime: number) => Promise<readonly FundingRate[]>;
  switch (market.exchange) {
    case 'Binance':
      crawlFunc = crawlBinanceFundingRate;
      break;
    case 'BitMEX':
      crawlFunc = crawlBitMEXFundingRate;
      break;
    case 'Huobi':
      crawlFunc = crawlHuobiFundingRate;
      break;
    case 'OKEx':
      crawlFunc = crawlOKExFundingRate;
      break;
    default:
      throw new Error(`Unknown exchange ${market.exchange}`);
  }

  let fundingRates: readonly FundingRate[] = [];
  let succeeded = false;
  while (!succeeded) {
    // eslint-disable-next-line no-await-in-loop
    const tmp = await crawlFunc(market, startTime).catch((error: Error) => error);
    if (tmp instanceof Error) {
      succeeded = false;
      console.error(`${market.exchange}-${market.pair}`);
      console.error(tmp.message);
      await new Promise((resolve) => setTimeout(resolve, 5000)); // eslint-disable-line no-await-in-loop
    } else {
      succeeded = true;
      fundingRates = tmp;
    }
  }

  const allFundingRates = _.sortBy(
    _.uniqBy(fundingRatesHistory.concat(fundingRates), 'fundingTime'),
    'fundingTime',
  );

  fs.writeFileSync(fundingRatesFile, `${JSON.stringify(allFundingRates, null, 2)}\n`);
}

(async (): Promise<void> => {
  await Promise.all(
    SWAP_EXCHANGES.map(async (exchange) => {
      const swapMarkets = await fetchMarkets(exchange, 'Swap');

      return Promise.all(swapMarkets.map((market) => crawlFundingRates(market)));
    }),
  );
})();
