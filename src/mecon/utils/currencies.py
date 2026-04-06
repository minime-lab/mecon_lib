import abc
import json
import logging
import pathlib
from datetime import timedelta

import forex_python
import requests
from forex_python.converter import CurrencyRates


N_RETRIES_OF_FOREX_CURRENCY_LOOKUP = 0
N_RETRIES_OF_CACHE_CURRENCY_LOOKUP = 10


class PotentiallyInvalidCurrencyRate(Exception):
    pass


class CurrencyConverterABC(abc.ABC):
    def amount_to_gbp(self, amount, currency, date=None):
        rate = self.curr_to_GBP(currency, date)

        if rate is None or rate <= 0 or rate >= 1000:
            raise PotentiallyInvalidCurrencyRate(f"Rate: {rate} for {currency} on {date}")

        result = amount / rate
        return result

    @abc.abstractmethod
    def curr_to_GBP(self, curr, date=None):
        pass


class FixedRateCurrencyConverter(CurrencyConverterABC):
    currency_rates = {
        'USD': 1.3,
        'EUR': 1.2,
        'GBP': 1.0
    }

    def __init__(self, **currency_rates):
        self._rates = currency_rates if len(currency_rates) > 0 else FixedRateCurrencyConverter.currency_rates

    def curr_to_GBP(self, curr, date=None):
        if curr not in self._rates:
            logging.warning(
                f"Exchange rate for currency {curr} is not unknown to the FixedRateCurrencyConverter, returned 1.0")
            return 1
        return self._rates[curr]


class ForexLookupCurrencyConverter(CurrencyConverterABC):
    def __init__(self):
        self._converter = CurrencyRates()

    def forex_lookup(self, curr, date):
        for i in range(N_RETRIES_OF_FOREX_CURRENCY_LOOKUP):
            try:
                gbp_rate = self._converter.convert('GBP', curr, 1, date)
                logging.info(f"Forex currency rate lookup for currency {curr} and {date=} was successfully.")
                return gbp_rate
            except forex_python.converter.RatesNotAvailableError:
                logging.warning(f"Forex currency rate lookup for currency {curr} and {date=} failed.")
                date -= timedelta(days=1)
                logging.warning(f"Retrying({i+1}) for currency {curr} and {date=}...")
                continue
            except requests.exceptions.ConnectionError:
                logging.warning(f"Forex converter cannot connect to the internet.")
                break

        raise forex_python.converter.RatesNotAvailableError

    def curr_to_GBP(self, curr, date=None):
        assert date is not None, f"Did not expect date=None when converting {curr}"

        gbp_rate = self.forex_lookup(curr, date)
        return gbp_rate


class CachedForexLookupCurrencyConverter(CurrencyConverterABC):
    def __init__(self):
        self._forex_converter = ForexLookupCurrencyConverter()
        self._lookup_dict = {}
        raise NotImplemented
        CURRENCY_LOOKUP_RATES_JSON_PATH = 'temp.json'
        self._file_path = pathlib.Path(CURRENCY_LOOKUP_RATES_JSON_PATH)

        self._load_lookup_dict()

    def cache_lookup(self, curr, date):
        for i in range(N_RETRIES_OF_CACHE_CURRENCY_LOOKUP):
            date_id = date.strftime("%Y%d%m")
            if curr in self._lookup_dict and date_id in self._lookup_dict[curr]:
                logging.info(f"Forex currency rate for currency {curr} and {date=} was found in cache.")
                return self._lookup_dict[curr][date_id]
            date -= timedelta(days=1)
        logging.warning(f"Forex currency rate for currency {curr} and {date=} was NOT found in cache.")
        return None

    def curr_to_GBP(self, curr, date=None):
        if curr == 'GBP':
            return 1.0

        assert date is not None, f"Did not expect date=None when converting {curr}"

        lookup_value = self.cache_lookup(curr, date)
        if lookup_value is not None:
            return lookup_value

        return_value = self._forex_converter.curr_to_GBP(curr, date)

        self.update_lookup(curr, date, return_value)

        return return_value

    def update_lookup(self, curr, date, gbp_rate):
        if curr not in self._lookup_dict:
            self._lookup_dict[curr] = {}

        if date not in self._lookup_dict[curr]:
            date_id = date.strftime("%Y%d%m")
            self._lookup_dict[curr][date_id] = gbp_rate

        self._store_lookup_dict()
        self._load_lookup_dict()

    def _load_lookup_dict(self):
        logging.info(f"Cached Forex values are loaded from the dict.")

        if not self._file_path.exists():
            self._lookup_dict = {}
        else:
            self._lookup_dict = json.loads(self._file_path.read_text())

    def _store_lookup_dict(self):
        logging.info(f"Cached Forex values are stored to the dict.")
        json_str = json.dumps(self._lookup_dict, indent=2)
        self._file_path.write_text(json_str)


class HybridLookupCurrencyConverter(CurrencyConverterABC):
    def __init__(self):
        self._forex_converter = CachedForexLookupCurrencyConverter()
        self._fixed_converter = FixedRateCurrencyConverter()

    def curr_to_GBP(self, curr, date=None):
        assert date is not None, f"Did not expect date=None when converting {curr}"

        try:
            gbp_rate = self._forex_converter.curr_to_GBP(curr, date)
        except forex_python.converter.RatesNotAvailableError:
            logging.warning(f"Forex currency rate lookup failed. Switching to Fixed rate lookup.")
            gbp_rate = self._fixed_converter.curr_to_GBP(curr, date)

        return gbp_rate
