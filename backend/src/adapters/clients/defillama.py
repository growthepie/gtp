## https://github.com/coindataschool/defillama2/blob/main/defillama2/defillama2.py
## MSeidl 01.02.2023 - added by_chain param to _tidy_frame_volume_this_dex and get_daily_volumes_this_dex
## MSeidl 08.02.2023 - added to get_stablecoin_hist_mcap_on_a_chain() and get_stablecoin_hist_mcap()

import requests
import pandas as pd
import numpy as np
import time
from urllib.parse import urlencode, quote

TVL_BASE_URL = VOLUMES_BASE_URL = FEES_BASE_URL = "https://api.llama.fi"
COINS_BASE_URL = "https://coins.llama.fi"
STABLECOINS_BASE_URL = "https://stablecoins.llama.fi"
YIELDS_BASE_URL = "https://yields.llama.fi"
ABI_DECODER_BASE_URL = "https://abi-decoder.llama.fi"
BRIDGES_BASE_URL = "https://bridges.llama.fi"

class DefiLlama:
    """ 
    Implements methods for calling DeFiLlama APIs and cleaning returned data. 
    """

    def __init__(self):
        self.session = requests.Session()

    def _get(self, api_name, endpoint, params=None):
        """Send 'GET' request.

        Parameters
        ----------
        api_name : string
            Possible values are 'TVL', 'COINS', 'STABLECOINS', 'YIELDS', 
            'VOLUMES', and 'ABI_DECODER'. Each has a different base url.
        endpoint : string 
            Endpoint to be added to base URL.
        params : dictionary
            HTTP request parameters.
        
        Returns
        -------
        JSON response
        """
        if api_name == 'TVL':
            url = TVL_BASE_URL + endpoint
        elif api_name == 'COINS':
            url = COINS_BASE_URL + endpoint  
        elif api_name == 'STABLECOINS':    
            url = STABLECOINS_BASE_URL + endpoint
        elif api_name == 'YIELDS':
            url = YIELDS_BASE_URL + endpoint 
        elif api_name == 'VOLUMES':
            url = VOLUMES_BASE_URL + endpoint
        elif api_name == 'FEES':
            url = FEES_BASE_URL + endpoint
        elif api_name == 'BRIDGES':
            url = BRIDGES_BASE_URL + endpoint
        else: 
            url = ABI_DECODER_BASE_URL + endpoint
        return self.session.request('GET', url,params=params,timeout=30).json()

    # --- TVL --- #
    
    def _tidy_frame_tvl(self, df):
        """Set `date` of input data frame as index and shorten TVL column name.
        
        Parameters
        ----------
        df : data frame 
            Must contains two columns: `date` and 'totalLiquidityUSD'.
        Returns 
        -------
        data frame
        """
        df['date'] = pd.to_datetime(df['date'], unit='s', utc=True)
        df = df.set_index('date').rename(columns={'totalLiquidityUSD': 'tvl'})
        return df

    def get_protocol_curr_tvl(self, protocol):
        """Get current TVL of a protocol.

        Parameters
        ----------
        protocol : string
            Protocol name.
        
        Returns 
        -------
        float
        """
        resp = self._get('TVL', f'/tvl/{protocol}')
        return resp['lastHourlyRecord']

    def get_chains_curr_tvl(self):
        """Get current TVL of all chains.
        
        Returns 
        -------
        data frame
        """
        resp = self._get('TVL', f'/chains/')
        df = pd.DataFrame(resp).loc[:, ['name', 'tokenSymbol', 'tvl']]
        df = df.rename(columns={'name':'chain', 'tokenSymbol':'token'})
        return df.reset_index(drop=True)

    def get_defi_hist_tvl(self):
        """Get historical TVL of DeFi on all chains.

        Returns 
        -------
        data frame
        """
        resp = self._get('TVL', '/charts')
        df = pd.DataFrame(resp)
        return self._tidy_frame_tvl(df)

    def get_chain_hist_tvl(self, chain):
        """Get historical TVL of a chain.

        Parameters
        ----------
        chain : string
            Chain name.
        
        Returns 
        -------
        data frame
        """
        resp = self._get('TVL', f'/charts/{chain}')
        df = pd.DataFrame(resp)
        return self._tidy_frame_tvl(df)
        
    def get_protocols(self):
        """Get detailed information on all protocols. 
        
        Returns 
        -------
        data frame
        """
        return pd.DataFrame(self._get('TVL', '/protocols'))

    def get_protocols_fundamentals(self):
        """Get current TVL, MCap, FDV, 1d and 7d TVL % change on all protocols.
        
        Parameters
        ----------
        protocol : string
            Protocol name.
        
        Returns 
        -------
        data frame
        """
        df = pd.DataFrame(self._get('TVL', '/protocols'))
        cols = ['name', 'symbol', 'chain', 'category', 'chains', 
                'tvl', 'change_1d', 'change_7d', 
                'fdv', 'mcap', 'forkedFrom']
        df = df.loc[:, cols].rename(columns={'forkedFrom':'forked_from'})
        return df

    def get_protocol(self, protocol):
        """Get detailed info on a protocol and breakdowns by token and chain.
        
        Parameters
        ----------
        protocol : string
            Protocol name.
        
        Returns 
        -------
        dictionary
        """
        return self._get('TVL', f'/protocol/{protocol}')

    def get_protocol_curr_tvl_by_chain(self, protocol):
        """Get current TVL of a protocol.

        Parameters
        ----------
        protocol : string
            Protocol name.
        
        Returns 
        -------
        data frame
        """
        dd = self.get_protocol(protocol)['currentChainTvls']
        if 'staking' in dd:
            dd.pop('staking')
        ss = pd.Series(dd)
        ss.name='tvl'
        return ss.to_frame()
    
    def get_protocol_hist_tvl_by_chain(self, protocol):
        """Get historical TVL of a protocol by chain.

        Parameters
        ----------
        protocol : string
            Protocol name.
        
        Returns 
        -------
        dict of data frames
        """
        dd = self.get_protocol(protocol)
        d1 = dd['currentChainTvls']
        if 'staking' in d1:
            d1.pop('staking')
        chains = list(d1.keys())
        return {chain: self._tidy_frame_tvl(
            pd.DataFrame(dd['chainTvls'][chain]['tvl'])) for chain in chains}

    # --- coins --- #
    
    def _tidy_frame_price(self, resp):
        """ Convert json resp (dict) of snapshot prices to data frame. """
        ha = pd.DataFrame([item.split(':') for item in resp['coins'].keys()])
        ha.columns = ['chain', 'token_address']
        df = ha.join(pd.DataFrame(resp['coins'].values()))
        # convert unix time (seconds) to utc datetime and use it as index
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        return df

    def _tidy_frame_hist_batch_prices(self, resp):
        """ Convert json resp (dict) of batch prices to data frame. """
        # extract chains and token addrs and put in a data frame
        dfl = pd.DataFrame([item.split(':') for item in resp['coins'].keys()])
        dfl.columns = ['chain', 'token_address']
        # extract prices for all tokens and all timestamps
        lst = list(resp['coins'].values())
        # add token symbol as a col to dfl to merge with dfr later
        dfl['symbol'] = [elt['symbol'] for elt in lst]
        # flatten the nested list of prices for all tokens and all timestamps 
        # into a data frame
        dfr = pd.json_normalize(lst, record_path='prices', meta='symbol')
        # convert unix time (seconds) to utc datetime
        dfr['timestamp'] = pd.to_datetime(dfr['timestamp'], unit='s', utc=True)
        # merge on the symbol col
        return pd.merge(dfl, dfr)

    def get_tokens_curr_prices(self, token_addrs_n_chains):
        """Get current prices of tokens by contract address.

        Parameters
        ----------
        token_addrs_n_chains : dictionary
            Each key is a token address; each value is a chain where the token 
            address resides. If getting price from coingecko, use token name as 
            key and 'coingecko' as value. For example, 
            {'0xdF574c24545E5FfEcb9a659c229253D4111d87e1':'ethereum',
             'ethereum':'coingecko'}

        Returns 
        -------
        data frame
        """
        ss = ','.join([v + ':' +k for k, v in token_addrs_n_chains.items()])
        resp = self._get('COINS', f'/prices/current/{ss}')
        df = self._tidy_frame_price(resp)
        df = df.set_index('timestamp')
        return df.loc[:, ['symbol','price','chain','decimals','token_address']] 

    def get_tokens_earliest_prices(self, token_addrs_n_chains):
        """Get earliest timestamp price record for tokens.

        Parameters
        ----------
        token_addrs_n_chains : dictionary
            Each key is a token address; each value is a chain where the token 
            address resides. If getting price from coingecko, use token name as 
            key and 'coingecko' as value. For example, 
            {'0xdF574c24545E5FfEcb9a659c229253D4111d87e1':'ethereum',
             'ethereum':'coingecko'}

        Returns 
        -------
        data frame
        """
        ss = ','.join([v + ':' +k for k, v in token_addrs_n_chains.items()])
        resp = self._get('COINS', f'/prices/first/{ss}')
        df = self._tidy_frame_price(resp)
        df = df.rename(columns={'timestamp':'earliest_timestamp'})
        return df.loc[:, ['symbol','chain','earliest_timestamp',
                          'price','token_address']]

    def get_tokens_hist_snapshot_prices(self, token_addrs_n_chains, timestamp):
        """Get historical snapshot prices of tokens by contract address.

        Parameters
        ----------
        token_addrs_n_chains : dictionary
            Each key is a token address; each value is a chain where the token 
            address resides. If getting price from coingecko, use token name as 
            key and 'coingecko' as value. For example, 
            {'0xdF574c24545E5FfEcb9a659c229253D4111d87e1':'ethereum',
             'ethereum':'coingecko'}
        timestamp : string
            Human-readable timestamp in utc, for example, '2021-09-25 00:27:53'

        Returns 
        -------
        data frame
        """
        ss = ','.join([v + ':' +k for k, v in token_addrs_n_chains.items()])
        unix_ts = pd.to_datetime(timestamp, utc=True).value / 1e9
        resp = self._get('COINS', f'/prices/historical/{unix_ts}/{ss}')
        df = self._tidy_frame_price(resp)
        df = df.set_index('timestamp')
        return df.loc[:, ['symbol','price','chain','token_address']]
        
    def get_tokens_hist_batch_prices(self, chain_token_addr_timestamps):
        """Get historical prices of tokens by chain at multiple timestamps.

        Parameters
        ----------
        chain_token_addr_timestamps : dictionary
            Each key is a chain:token_address; each value is a list of unix 
            timestamps in seconds. For example, 
            {"avax:0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": 
                [1666876743, 1666862343],
             "coingecko:ethereum": [1666869543, 1666862343]}

        Returns 
        -------
        data frame
        """
        val = str(chain_token_addr_timestamps).replace("'", '"')
        param = urlencode(dict(coins=val), quote_via=quote)
        resp = self._get('COINS', '/batchHistorical/', params = param)
        df = self._tidy_frame_hist_batch_prices(resp)
        df = df.set_index('timestamp')
        return df.loc[:, ['symbol','price','chain','token_address']] 

    def get_daily_open_close(self, token_addrs_n_chains, start, end, kind='close'):
        """Get historical daily open and close prices of tokens by contract 
        address. Data on both the starting and end dates are included. 
        
        Parameters
        ----------
        token_addrs_n_chains : dictionary
            Each key is a token address; each value is a chain where the token 
            address resides. If getting price from coingecko, use token name as 
            key and 'coingecko' as value. For example, 
            {'0xdF574c24545E5FfEcb9a659c229253D4111d87e1':'ethereum',
             'ethereum':'coingecko'}
        start : string
            Start date, for example, '2021-01-01'
        end : string
            End date, for example, '2022-01-01'
        kind : string
            Either 'close' (default, at 23:59:59) or 'open' (at 00:00:00). Does 
            NOT support other values at the moment.

        Returns 
        -------
        data frame
        """
        start = pd.to_datetime(start, format='%Y-%m-%d', utc=True)
        end   = pd.to_datetime(end, format='%Y-%m-%d', utc=True) 
        today = pd.to_datetime('today', utc=True).date()
        if (end.date() == today) and (kind == 'close'): 
            end -= pd.Timedelta(days=1)
        dates = pd.date_range(start, end) # , inclusive='left'

        # get unix seconds for each date 
        if kind == 'close':
            dttms = [dt.replace(hour=23, minute=59, second=59).timestamp() 
                     for dt in dates]
        elif kind == 'open':
            dttms = [dt.replace(hour=0, minute=0, second=0).timestamp() 
                     for dt in dates]
        else: 
            raise Exception("Only 'open' or 'close' are supported.")

        # necessary due to api limit
        chunk_size = 30 # 30 days
        if len(dttms) <= chunk_size:
            # make input dict for getting historical batch prices
            dd = {f'{v}:{k}':dttms for k, v in token_addrs_n_chains.items()}            
            df = self.get_tokens_hist_batch_prices(dd)
        else: # break into batches and iterate due to API call limit
            nchunks = int(np.ceil(len(dttms) / chunk_size))
            lst = list()
            for i in range(nchunks):
                # i = 11
                # make input dict for getting historical batch prices
                dd = {f'{v}:{k}':dttms[i*chunk_size:(i+1)*chunk_size]
                      for k, v in token_addrs_n_chains.items()}
                # download historical prices at these time points
                lst.append(self.get_tokens_hist_batch_prices(dd))
                time.sleep(0.1)
                # print(len(lst))
            df = pd.concat(lst, axis=0)

        # clean data so that the resulting frame has 
        #   - each row is a date
        #   - each column is a token
        #   - each value is a price (open or close)
        df = df.reset_index()
        if kind == 'close':
            df['date'] = np.where(df.timestamp.dt.hour == 0, 
                                  df.timestamp.dt.date - pd.Timedelta(days=1), 
                                  df.timestamp.dt.date)
        if kind == 'open':
            df['date'] = np.where(df.timestamp.dt.hour == 0, 
                                  df.timestamp.dt.date, 
                                  df.timestamp.dt.date + pd.Timedelta(days=1))
        df = df.groupby(['date', 'symbol'])['price'].mean()
        df = df.reset_index().pivot(index='date', columns='symbol', values='price')
        df.columns.name = None
        df.index = pd.to_datetime(df.index, utc=True).date # date is an attribute
        # here, and calling date() as a method throws error. 
        df.index.name='date'
        return df

    def get_tokens_hist_prices(self, token_addrs_n_chains, start, end, freq='hourly'):
        """Get historical hourly or daily prices of tokens by contract address. 
        Data on both the starting and end dates are included. If you only want 
        daily open/close prices, use get_daily_open_close().
        
        Parameters
        ----------
        token_addrs_n_chains : dictionary
            Each key is a token address; each value is a chain where the token 
            address resides. If getting price from coingecko, use token name as 
            key and 'coingecko' as value. For example, 
            {'0xdF574c24545E5FfEcb9a659c229253D4111d87e1':'ethereum', 
             'ethereum':'coingecko'}
        start : string
            Start date, for example, '2022-11-01'
        end : string
            End date, for example, '2022-11-30'. 
        freq : string
            Data granularity, 'hourly' (default) or 'daily'. Does NOT support 
            other values at the moment.

        Returns 
        -------
        data frame
        """
        start = pd.to_datetime(start, format='%Y-%m-%d', utc=True)
        end   = pd.to_datetime(end, format='%Y-%m-%d', utc=True) + pd.Timedelta(days=1)
        now   = pd.to_datetime('now', utc=True)
        dttms = pd.date_range(start, end, inclusive='left', freq='60min')
        # convert to unix seconds and 
        # give 4 hours buffer to ensure DeFiLlama data are available
        dttms = [dttm.timestamp() for dttm in dttms 
                 if dttm < now-pd.Timedelta(hours=4)] 
        
        # necessary due to api limit
        chunk_size = 24*2 # 2 days
        if len(dttms) <= chunk_size:
            # make input dict for getting historical batch prices
            dd = {f'{v}:{k}':dttms for k, v in token_addrs_n_chains.items()}
            df = self.get_tokens_hist_batch_prices(dd)
        else: # break into batches and iterate due to API call limit
            nchunks = int(np.ceil(len(dttms) / chunk_size))
            lst = list()
            for i in range(nchunks):
                # i = 11
                # make input dict for getting historical batch prices
                dd = {f'{v}:{k}':dttms[i*chunk_size:(i+1)*chunk_size]
                      for k, v in token_addrs_n_chains.items()}
                # download historical prices at these time points
                lst.append(self.get_tokens_hist_batch_prices(dd))
                time.sleep(0.1)
                # print(len(lst))
            df = pd.concat(lst, axis=0)
        
        # clean data so that the resulting frame has 
        #   - each row is a datetime
        #   - each column is a token
        #   - each value is a price (open or close)
        df = df.reset_index()
        df['datetime'] = [elt.round(freq='H') for elt in df['timestamp']]
        # df[['timestamp', 'datetime']].head(10)
        # `datetime` can have duplicates, so take their avg price
        df = df.groupby(['datetime', 'symbol']).agg({'price':'mean'})
        df = df.reset_index()\
            .pivot(index='datetime', columns='symbol', values='price')
        df.columns.name = None
        df.index = pd.to_datetime(df.index, utc=True)
        
        # derive daily prices if user requests them instead of hourly data
        if freq == 'daily':
            symbols = df.columns
            # calculate daily prices using hourly data
            daily_open = df.asfreq('1d')
            daily_low = df.resample('D').min()
            daily_high = df.resample('D').max()
            daily_close = daily_open.shift(-1)
            daily_med = df.resample('D').median()
            daily_avg = df.resample('D').mean()
            daily_std = df.resample('D').std()
            # assign header 
            daily_open.columns = pd.MultiIndex.from_tuples(
                [('open', symbol) for symbol in symbols])
            daily_low.columns = pd.MultiIndex.from_tuples(
                [('low', symbol) for symbol in symbols])
            daily_high.columns = pd.MultiIndex.from_tuples(
                [('high', symbol) for symbol in symbols])
            daily_close.columns = pd.MultiIndex.from_tuples(
                [('close', symbol) for symbol in symbols])
            daily_med.columns = pd.MultiIndex.from_tuples(
                [('median', symbol) for symbol in symbols])
            daily_avg.columns = pd.MultiIndex.from_tuples(
                [('mean', symbol) for symbol in symbols])
            daily_std.columns = pd.MultiIndex.from_tuples(
                [('std', symbol) for symbol in symbols])
            # join together
            df = pd.concat([daily_open, daily_low, daily_high, daily_close, 
                            daily_med, daily_avg, daily_std], axis=1)
            # change index from DateTime to Date
            df.index = pd.to_datetime(df.index, utc=True).date # date is an 
            # attribute here, and calling the date() method throws error. 
            df.index.name='date'
        return df
    
    def get_prices_at_regular_intervals(self, token_addrs_n_chains, end,
                                        end_format=None, span=30, period='4h'):
        """Get prices of tokens before user-supplied end time at regular intervals.

        Parameters
        ----------
        token_addrs_n_chains : dictionary
            Each key is a token address; each value is a chain where the token 
            address resides. If getting price from coingecko, use token name as 
            key and 'coingecko' as value. For example, 
            {'0xdF574c24545E5FfEcb9a659c229253D4111d87e1':'ethereum',
             'ethereum':'coingecko'}
        end : str
            Datetime string. For example, '2022-12-01' or '2012-12-01 08:15:00'
        end_format : str
            Datetime string format for parsing `end`. For example, 
            '%Y-%m-%d' or '%Y-%m-%d %H:%M:%S'.
        span : int
            Number of price points, defaults to 30.
        period : str
            Duration between data points, defaults to '4h'. Can use regular 
            chart candle notion like '4h' etc where: W = week, D = day, 
            H = hour, M = minute (not case sensitive).
            
        Returns 
        -------
        data frame
        """
        ss = ','.join([v + ':' +k for k, v in token_addrs_n_chains.items()])
        unix_sec = pd.to_datetime(end, format=end_format, utc=True).timestamp()
        param = dict(end=unix_sec, period=period, span=span)
        param = urlencode(param, quote_via=quote)
        base_url = "https://coins.llama.fi"
        url = base_url + f'/chart/{ss}?'
        resp = requests.Session()\
            .request('GET', url, params=param, timeout=30)\
            .json()
        df = self._tidy_frame_hist_batch_prices(resp)
        df = df.groupby(['timestamp', 'symbol'])\
                .agg({'price':'mean'})\
                .reset_index()\
                .pivot(index='timestamp', columns='symbol', values='price')
        df.columns.name = None
        return df
    
    # no need to implement /percentage/{coins} cuz users can calculate 
    # % change using prices downloaded via the other functions.
    
    def get_closest_block(self, chain, timestamp):
        """Get the closest block to a timestamp.

        Parameters
        ----------
        chain : string
            Name of the chain.
        timestamp : string
            Human-readable timestamp in utc, for example, '2021-09-25 00:27:53'.

        Returns 
        -------
        data frame
        """
        unix_sec = pd.to_datetime(timestamp, utc=True).timestamp()
        resp = self._get('COINS', f'/block/{chain}/{unix_sec}')
        df = pd.DataFrame(resp, index=range(1))
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        return df
    
    # --- stablecoins --- #
    
    def get_stablecoins_circulating(self, include_price=False):
        """Get the circulating amounts for all stablecoins.

        Parameters
        ----------
        include_price : logical (default=False)
            Whether to include current stablecoin prices. Seems like it doesn't
            do anything and the returned data doesn't return current price even 
            set to True.

        Returns 
        -------
        data frame
        """
        resp = self._get('STABLECOINS', 
                         f'/stablecoins?includePrices={include_price}')
        lst = resp['peggedAssets']
        
        res = []
        for d0 in lst:
            _ = d0.pop('chainCirculating')
            _ = d0.pop('chains')
            res.append(pd.DataFrame(d0).reset_index(drop=True))
        df = pd.concat(res)
        df['id'] = df.id.astype(int)
        df = df.set_index('id')
        return df

    def get_stablecoins_circulating_by_chain(self, include_price=False):
        """Get the circulating amounts for all stablecoins, broken down by chain.

        Parameters
        ----------
        include_price : logical (default=False)
            Whether to include current stablecoin prices. Seems like it doesn't
            do anything and the returned data doesn't return current price even 
            set to True.

        Returns 
        -------
        dictionary where the keys are stablecoin symbols and values are data frames.
        """
        resp = self._get('STABLECOINS', 
                         f'/stablecoins?includePrices={include_price}')
        lst = resp['peggedAssets']
        
        dict_of_dfs = dict()
        for d0 in lst:
            d1 = d0.pop('chainCirculating')
            _  = d0.pop('chains')
            haha = []
            for k, v in d1.items():
                da = pd.DataFrame(v)
                da['chain'] = k
                da = da.reset_index()\
                       .rename(columns={'index':'type'})\
                       .set_index('chain')
                haha.append(da)
            dict_of_dfs[d0['symbol']] = pd.concat(haha)
        return dict_of_dfs

    def get_stablecoin_hist_mcap(self, id):
        """Get all available historical mcap values for a stablecoin.

        Parameters
        ----------
        id : int 
            Stablecoin ID, you can get these from get_stablecoins_circulating().
            For example, USDT has id 1, USDC 2, Frax 6.

        Returns 
        -------
        data frame
        """
        resp = self._get('STABLECOINS', 
                         f'/stablecoincharts/all?stablecoin={id}')
        df = pd.concat([pd.DataFrame(d) for d in resp])        
        df['date'] = pd.to_datetime(df['date'], unit='s', utc=True)
        df.reset_index(inplace=True)
        df = df[df['index']=='peggedUSD']
        df = df[['date', 'totalCirculating']]
        df = df.set_index('date')
        return df

    def get_stablecoin_hist_mcap_on_a_chain(self, id, chain):
        """Get all available historical mcap values for a stablecoin on a 
        particular chain.

        Parameters
        ----------
        id : int 
            Stablecoin ID, you can get it from get_stablecoins_circulating().
            For example, USDT has id 1, USDC 2, Frax 6.
        chain : string
            Name of the chain where the stablecoin resides.

        Returns 
        -------
        data frame
        """
        resp = self._get('STABLECOINS', 
                         f'/stablecoincharts/{chain}?stablecoin={id}')
        df = pd.concat([pd.DataFrame(d) for d in resp])        
        df['date'] = pd.to_datetime(df['date'], unit='s', utc=True)
        df.reset_index(inplace=True)
        df = df[df['index']=='peggedUSD']
        df = df[['date', 'totalCirculating']]
        df = df.set_index('date')
        return df

    def get_stablecoins_curr_mcap_by_chain(self):
        """Get current mcap sum of all stablecoins on each chain.

        Returns 
        -------
        data frame
        """
        resp = self._get('STABLECOINS', f'/stablecoinchains')
        df = pd.concat([pd.DataFrame(d) for d in resp])
        df = df.reset_index().rename(columns={'index':'type'})
        df = df.set_index('name').drop(['gecko_id', 'tokenSymbol'], axis=1)
        df.index.name = 'chain'
        return df

    def get_stablecoins_prices(self):
        """Get historical prices of all stablecoins.

        Returns 
        -------
        data frame
        """
        resp = self._get('STABLECOINS', f'/stablecoinprices')
        df = pd.concat([pd.DataFrame(d) for d in resp])
        df = df.reset_index().rename(columns={'index':'stablecoin'})
        df['date'] = pd.to_datetime(df['date'], unit='s', utc=True)
        df = df.set_index('date')
        return df

    # no need to implement /stablecoin/{asset} cuz it just returns all data in 
    # a deeply nested list that other api endpoints return separately.

    # --- yields --- #
    
    def get_pools_yields(self):
        """Get the latest data for all pools, including enriched info such as 
        predictions.

        Returns 
        -------
        data frame
        """
        resp = self._get('YIELDS', f'/pools')
        lst = resp['data']
        df = pd.json_normalize(lst)
        df.columns = df.columns.str.replace('predictions.', '', regex=False)
        df['apyPct30D'] = df.apyPct30D.astype(float)
        return df

    def get_pool_hist_apy(self, pool_id):
        """Get historical APY and TVL of a pool.

        Parameters
        ----------
        pool_id : str 
            Pool id, you can get it from the `pool` column after calling 
            get_pools_yields().

        Returns 
        -------
        data frame
        """
        resp = self._get('YIELDS', f'/chart/{pool_id}')
        df = pd.DataFrame(resp['data'])
        df['date'] = pd.to_datetime(
            df['timestamp'],
            format='%Y-%m-%dT%H:%M:%S.%f%z').dt.normalize()
        df = df.drop(columns='timestamp')
        # convert numeral strings to float
        numstr_cols = df.select_dtypes(include=['object']).columns
        df[numstr_cols] = df[numstr_cols].astype(float)
        # daily avg
        df = df.groupby('date').agg('mean')
        return df

    # --- volumes --- #

    def _tidy_frame_volume(self, resp):
        """ Convert json resp (dict) of dexes volumes to dict of data frames. """
        # overall volume across all dexes and chains
        volume_overall = pd.DataFrame(
            dict(total24h = resp['total24h'], 
                 total7d = resp['total7d'],
                 change_1d = resp['change_1d'],
                 change_7d = resp['change_7d'],
                 change_1m = resp['change_1m'],
                 change_7dover7d = resp['change_7dover7d']),
            index=range(1))
        # volume by dex
        df = pd.DataFrame(resp['protocols'])\
            .query("latestFetchIsOk == True & disabled == False")
        ha = df['breakdown24h']
        volume_by_dex = df.drop(columns=[
            'latestFetchIsOk', 'disabled', 'module', 'logo', 'protocolType', 
            'displayName', 'methodology', 'methodologyURL', 'breakdown24h', 
            'protocolsStats'])
        # volume by dex by chain
        volume_by_dex_by_chain = \
            pd.concat([pd.DataFrame(ha.iloc[i]) for i in range(len(ha))])\
            .stack().reset_index()
        volume_by_dex_by_chain.columns = ['protocol', 'chain', 'total24h']
        # daily volume of all dexes
        daily_volume = pd.DataFrame(resp['totalDataChart'])
        daily_volume.columns = ['date', 'volume']
        daily_volume['date'] = \
            pd.to_datetime(daily_volume['date'], unit='s', utc=True)
        daily_volume = daily_volume.set_index('date')
        # daily volume by dex
        daily_volume_by_dex = pd.DataFrame(resp['totalDataChartBreakdown'])
        daily_volume_by_dex.columns = ['date', 'dex_vol_dict']
        daily_volume_by_dex['date'] = \
            pd.to_datetime(daily_volume_by_dex['date'], unit='s', utc=True)
        daily_volume_by_dex = daily_volume_by_dex[['date']]\
            .join(pd.DataFrame(daily_volume_by_dex['dex_vol_dict'].tolist()))
        daily_volume_by_dex = daily_volume_by_dex.set_index('date')
        return {'volume_overall': volume_overall, 
                'volume_by_dex': volume_by_dex, 
                'volume_by_dex_by_chain_24h': volume_by_dex_by_chain, 
                'daily_volume': daily_volume, 
                'daily_volume_by_dex': daily_volume_by_dex}
    
    def _tidy_frame_volume_this_dex(self, resp, by_chain=False):
        """ Convert json resp (dict) of a dex volumes to data frame. """
        if by_chain == True:
            df = pd.DataFrame(resp['totalDataChartBreakdown'], columns=['date','volume_dict'] )
            df_chains = df['volume_dict'].apply(pd.Series)
            cols = df_chains.columns.tolist()

            for col in cols:
                df_chains[col] = df_chains[col].apply(lambda x : sum(x.values()) if isinstance(x, dict) else x)

            df = pd.concat([df.drop(['volume_dict'], axis=1), df_chains], axis=1)
            df['date'] = pd.to_datetime(df['date'], unit='s', utc=True)
            df_dict = {}
            for col in df.columns.to_list()[1:]:
                df_dict[col] = df[['date', col]].rename(columns={col:'volume'})
            return df_dict
        else:
            df = pd.DataFrame(resp['totalDataChart'], columns=['sec', 'volume'])
            df['date'] = pd.to_datetime(df['sec'], unit='s', utc=True)
            df = df.drop(columns='sec').set_index('date')
            return df

    def get_dexes_volumes(self, data_type='dailyVolume'):
        """Get transaction volumes of all dexes, including 'Dexes', 
        'Derivatives', and 'Yield' protocols.

        Parameters
        ----------
        data_type : string
            Possible values are 'dailyVolume' or 'totalVolume'. It seems 
            'totalVolume' isn't used on DeFiLlama's website. So use 
            'dailyVolume' for most cases.
        
        Returns 
        -------
        dictionary of data frames: 
            - volume_overall
            - volume_by_dex 
            - volume_by_dex_by_chain_24h
            - daily_volume
            - daily_volume_by_dex
        """
        dd = dict(excludeTotalDataChart='false',
                  excludeTotalDataChartBreakdown='false',
                  dataType=data_type)
        param = urlencode(dd, quote_via=quote)
        resp = self._get('VOLUMES', '/overview/dexs', params = param)
        return self._tidy_frame_volume(resp)

    def get_dexes_volumes_this_chain(self, chain, data_type='dailyVolume'):
        """Get transaction volumes of all dexes, including 'Dexes', 
        'Derivatives', and 'Yield' protocols from a particular chain.

        Parameters
        ----------
        chain : string
            Name of blockchain. For example, 'ethereum'. List of all supported 
            chains can be found in the output of get_dexes_volumes().
        data_type : string
            Possible values are 'dailyVolume' or 'totalVolume'. It seems 
            'totalVolume' isn't used on DeFiLlama's website. So use 
            'dailyVolume' for most cases.
        
        Returns 
        -------
        dictionary of data frames: 
            - volume_overall
            - volume_by_dex 
            - volume_by_dex_by_chain_24h
            - daily_volume
            - daily_volume_by_dex
        """
        dd = dict(excludeTotalDataChart='false',
                  excludeTotalDataChartBreakdown='false',
                  dataType=data_type)
        param = urlencode(dd, quote_via=quote)
        resp = self._get('VOLUMES', f'/overview/dexs/{chain.lower()}', 
                         params = param)
        return self._tidy_frame_volume(resp)

    def get_daily_volumes_this_dex(self, dex, by_chain=False, data_type='dailyVolume'):
        """Get historical daily transaction volumes of a dex.

        Parameters
        ----------
        dex : string
            Name of dex. For example, 'uniswap'.
        data_type : string
            Possible values are 'dailyVolume' or 'totalVolume'. It seems 
            'totalVolume' isn't used on DeFiLlama's website. So use 
            'dailyVolume' for most cases.
        
        Returns 
        -------
        data frame 
        """
        dd = dict(excludeTotalDataChart='true',
                  excludeTotalDataChartBreakdown='false',
                  dataType=data_type)
        param = urlencode(dd, quote_via=quote)
        resp = self._get('VOLUMES', f'/summary/dexs/{dex}', params = param)
        return self._tidy_frame_volume_this_dex(resp, by_chain=by_chain)

    def get_options_dexes_volumes(self, data_type='dailyNotionalVolume'):
        """Get transaction volumes of all options dexes.

        Parameters
        ----------
        data_type : string
            Possible values are 'dailyNotionalVolume', 'dailyPremiumVolume',
            'totalNotionalVolume', or 'totalPremiumVolume'.
        
        Returns 
        -------
        dictionary of data frames: 
            - volume_overall
            - volume_by_dex 
            - volume_by_dex_by_chain_24h
            - daily_volume
            - daily_volume_by_dex
        """
        dd = dict(excludeTotalDataChart='false',
                  excludeTotalDataChartBreakdown='false',
                  dataType=data_type)
        param = urlencode(dd, quote_via=quote)
        resp = self._get('VOLUMES', '/overview/options', params = param)
        return self._tidy_frame_volume(resp)

    def get_options_dexes_volumes_this_chain(self, chain, 
                                             data_type='dailyNotionalVolume'):
        """Get transaction volumes of all options dexes from a particular chain.

        Parameters
        ----------
        chain : string
            Name of blockchain. For example, 'ethereum'. List of all supported 
            chains can be found in the output of get_dexes_volumes().
        data_type : string
            Possible values are 'dailyNotionalVolume', 'dailyPremiumVolume',
            'totalNotionalVolume', or 'totalPremiumVolume'.
        
        Returns 
        -------
        dictionary of data frames: 
            - volume_overall
            - volume_by_dex 
            - volume_by_dex_by_chain_24h
            - daily_volume
            - daily_volume_by_dex
        """
        dd = dict(excludeTotalDataChart='false',
                  excludeTotalDataChartBreakdown='false',
                  dataType=data_type)
        param = urlencode(dd, quote_via=quote)
        resp = self._get('VOLUMES', f'/overview/options/{chain.lower()}', 
                         params = param)
        return self._tidy_frame_volume(resp)

    def get_daily_volumes_this_options_dex(self, dex, 
                                           data_type='dailyNotionalVolume'):
        """Get historical daily transaction volumes of an options dex.

        Parameters
        ----------
        dex : string
            Name of options dex. For example, 'lyra', 'premia'.
        data_type : string
            Possible values are 'dailyNotionalVolume', 'dailyPremiumVolume',
            'totalNotionalVolume', or 'totalPremiumVolume'.
        
        Returns 
        -------
        data frame 
        """
        dd = dict(excludeTotalDataChart='true',
                  excludeTotalDataChartBreakdown='true',
                  dataType=data_type)
        param = urlencode(dd, quote_via=quote)
        resp = self._get('VOLUMES', f'/summary/options/{dex}', params = param)
        return self._tidy_frame_volume_this_dex(resp)
                
    # --- fees and revenue --- #
    
    def get_fees(self, data_type='dailyFees'):
        """Get fees paid to or fees accrued (revenue) by all protocols.

        Parameters
        ----------
        data_type : string
            Possible values are 'dailyFees', 'totalFees', 'dailyRevenue', or 
            'totalRevenue', where fees are paid by users whereas revenue is 
            fees accrued to the protocol. So fees != revenue here.
        
        Returns 
        -------
        dictionary of data frames: 
            - fees_overall (or revenue_overall)
            - fees_by_dex (or revenue_by_dex)
            - fees_by_dex_by_chain_24h (or revenue_by_dex_by_chain_24h)
            - daily_fees (or daily_revenue)
            - daily_fees_by_dex (or daily_revenue_by_dex)
        """
        dd = dict(excludeTotalDataChart='false',
                  excludeTotalDataChartBreakdown='false',
                  dataType=data_type)
        param = urlencode(dd, quote_via=quote)
        resp = self._get('FEES', '/overview/fees', params = param)
        dd_res = self._tidy_frame_volume(resp)
        if 'Fees' in data_type:
            new_keys = [k.replace('volume', 'fees') for k in dd_res.keys()]
        if 'Revenue' in data_type:
            new_keys = [k.replace('volume', 'revenue') for k in dd_res.keys()]
        return dict(zip(new_keys, dd_res.values()))
    
    def get_fees_this_chain(self, chain, data_type='dailyFees'):
        """Get fees paid to or fees accrued (revenue) by all protocols from a 
        particular chain.

        Parameters
        ----------
        chain : string
            Name of blockchain. For example, 'ethereum'. List of all supported 
            chains can be found in the output of get_fees().
        data_type : string
            Possible values are 'dailyFees', 'totalFees', 'dailyRevenue', or 
            'totalRevenue', where fees are paid by users whereas revenue is 
            fees accrued to the protocol. So fees != revenue here.
        
        Returns 
        -------
        dictionary of data frames: 
            - fees_overall (or revenue_overall)
            - fees_by_dex (or revenue_by_dex)
            - fees_by_dex_by_chain_24h (or revenue_by_dex_by_chain_24h)
            - daily_fees (or daily_revenue)
            - daily_fees_by_dex (or daily_revenue_by_dex)
        """
        dd = dict(excludeTotalDataChart='false',
                  excludeTotalDataChartBreakdown='false',
                  dataType=data_type)
        param = urlencode(dd, quote_via=quote)
        resp = self._get('FEES', f'/overview/fees/{chain.lower()}', params=param)
        dd_res = self._tidy_frame_volume(resp)
        if 'Fees' in data_type:
            new_keys = [k.replace('volume', 'fees') for k in dd_res.keys()]
        if 'Revenue' in data_type:
            new_keys = [k.replace('volume', 'revenue') for k in dd_res.keys()]
        return dict(zip(new_keys, dd_res.values()))
        
    def get_daily_fees_this_protocol(self, protocol, data_type='dailyFees'):
        """Get daily fees (paid by users) or revenue (accrued by the protocol) 
        of a protocol.

        Parameters
        ----------
        protocol : string
            Name of protocol. For example, 'gmx'.
        data_type : string
            Possible values are 'dailyFees', 'totalFees', 'dailyRevenue', or 
            'totalRevenue', where fees are paid by users whereas revenue is 
            fees accrued to the protocol. So fees != revenue here.
        
        Returns 
        -------
        data frame 
        """
        dd = dict(excludeTotalDataChart='true',
                  excludeTotalDataChartBreakdown='true',
                  dataType=data_type)
        param = urlencode(dd, quote_via=quote)
        resp = self._get('VOLUMES', f'/summary/fees/{protocol}', params = param)
        df = self._tidy_frame_volume_this_dex(resp)
        if 'Fees' in data_type:
            df.columns = df.columns.str.replace('volume', 'fees')
        if 'Revenue' in data_type:
            df.columns = df.columns.str.replace('volume', 'revenue')
        return df
        
    # --- bridges --- #
    
    def get_bridges_volumes(self):
        """Get all bridges along with summaries of recent bridge volumes.
        
        Returns 
        -------
        data frame 
        """
        resp = self._get('BRIDGES', f'/bridges')
        df = pd.DataFrame(resp['bridges'])\
            .drop(columns=['name', 'icon', 'chains', 'destinationChain'])
        df['chainsCnt'] = [len(dd['chains']) for dd in resp['bridges']]
        return df
    
    def get_bridge_volume(self, bridge_id):
        """Get volume summary of a particular bridge and volume breakdown by chain.

        Parameters
        ----------
        bridge_id : int
            Unique identifier of a bridge. For example, 1 is Polygon PoS. You 
            can look up all id values in the `id` column returned by calling
            `get_bridges_volumes()`.
        
        Returns 
        -------
        dictionary of data frames
        """
        resp = self._get('BRIDGES', f'/bridge/{bridge_id}')
        # bridge volume summary
        df = pd.DataFrame(resp)
        cols1 = ['displayName', 'lastHourlyVolume', 'currentDayVolume',
                'lastDailyVolume', 'dayBeforeLastVolume', 'weeklyVolume',
                'monthlyVolume']
        df1 = df[cols1].reset_index(drop=True).drop_duplicates()
        cols2 = ['lastHourlyTxs', 'currentDayTxs', 'prevDayTxs',
                'dayBeforeLastTxs', 'weeklyTxs', 'monthlyTxs']
        df2 = df[cols2].dropna().reset_index(drop=True).drop_duplicates()
        df_summary = df1.join(df2)
        
        # bridge volume summary by chain
        lst1 = []
        lst2 = []
        dd = resp['chainBreakdown']
        for k in dd.keys():
            df = pd.DataFrame(dd[k])
            cols1 = ['lastHourlyVolume', 'currentDayVolume', 'lastDailyVolume',
                    'dayBeforeLastVolume', 'weeklyVolume', 'monthlyVolume']
            cols2 = ['lastHourlyTxs', 'currentDayTxs', 'prevDayTxs', 
                    'dayBeforeLastTxs', 'weeklyTxs', 'monthlyTxs']
            da1 = df[cols1].reset_index(drop=True).drop_duplicates()    
            da2 = df[cols2].reset_index(names='txType')
            # insert a new col 'chain' to the left 
            da1.insert(0, 'chain', k) 
            da2.insert(0, 'chain', k)
            lst1.append(da1)
            lst2.append(da2)
        df_summary_by_chain = pd.concat(lst1, ignore_index=True)
        df_deposits_withdraws_by_chain = pd.concat(lst2, ignore_index=True)
        return {'summary':df_summary, 
                'summary_by_chain': df_summary_by_chain, 
                'deposits_withdraws_by_chain': df_deposits_withdraws_by_chain}

    def get_daily_volume_this_bridge(self, bridge_id, chain='all'):
        """Get historical volumes for a bridge on a particular chain or on all 
        chains.

        Parameters
        ----------
        bridge_id : int
            Unique identifier of a bridge. For example, 1 is Polygon PoS. You 
            can look up all id values in the `id` column returned by calling
            `get_bridges_volumes()`.
        chain : str
            Chain name. For example, 'Ethereum'. Default is 'all' for volume on 
            all chains.
        
        Returns 
        -------
        data frame
        """
        if chain != 'all':
            chain = chain.lower().capitalize()
        resp = self._get('BRIDGES', f'/bridgevolume/{chain}?id={bridge_id}')
        df = pd.DataFrame(resp)
        df['date'] = pd.to_datetime(df['date'], unit='s', utc=True)
        return df.set_index('date')

    def get_24h_token_volume_this_bridge(self, bridge_id, chain, date):
        """Get 24hr token and volume breakdown for a bridge. 

        Parameters
        ----------
        bridge_id : int
            Unique identifier of a bridge. For example, 1 is Polygon PoS. You 
            can look up all id values in the `id` column returned by calling
            `get_bridges_volumes()`.
        chain : str
            Chain name. For example, 'Ethereum'.
        date : str
            Date string of format '%Y-%m-%d', for example, '2022-12-01'. Data 
            returned will be for the 24hr period starting at 00:00 UTC on `date`.
        
        Returns 
        -------
        data frame
        """
        unix_sec = pd.to_datetime(date).timestamp()
        if chain != 'all':
            chain = chain.lower().capitalize()        
        resp = self._get('BRIDGES', 
                         f'/bridgedaystats/{unix_sec}/{chain}?id={bridge_id}') 
        # part 1
        df1 = pd.DataFrame(resp['totalTokensDeposited']).T.reset_index(drop=True)
        df1 = df1[['symbol', 'usdValue']]\
            .rename(columns={'usdValue':'TokensDeposited_usdValue'})
        # part 2
        df2 = pd.DataFrame(resp['totalTokensWithdrawn']).T.reset_index(drop=True)
        df2 = df2[['symbol', 'usdValue']]\
            .rename(columns={'usdValue':'TokensWithdrawn_usdValue'})
        # part 3
        df3 = pd.DataFrame(resp['totalAddressDeposited']).T.reset_index(drop=True)
        df3.columns = [f'AddressDeposited_{nm}' for nm in df3.columns]
        # part 4
        df4 = pd.DataFrame(resp['totalAddressWithdrawn']).T.reset_index(drop=True)
        df4.columns = [f'AddressWithdrawn_{nm}' for nm in df4.columns]
        return pd.merge(df1, df2).join(df3).join(df4)

    def get_tx_this_bridge(self, bridge_id, sourcechain, start, end, 
                           fromToAddrs_chains, limit=200):
        """Get all transactions for a bridge from a source chain within a date 
        range.

        Parameters
        ----------
        bridge_id : int
            Unique identifier of a bridge. For example, 1 is Polygon PoS. You 
            can look up all id values in the `id` column returned by calling
            `get_bridges_volumes()`.
        sourcechain : str
            Name of the chain bridging from. For example, 'Ethereum'.
        start : str
            Start of the date range. Date string of format '%Y-%m-%d', 
            for example, '2022-12-01'. 
        end : str
            End of the date range. Date string of format '%Y-%m-%d', 
            for example, '2022-12-01'. 
        fromToAddrs_chains : dict
            A dictionary with "from" or "to" addresses as keys and chain names 
            as values. Example chain names are ethereum, bsc, polygon, avax... .
        limit : int
            Number of transactions returned, maximum is 6000.
            
        Returns 
        -------
        data frame
        """
        start = pd.to_datetime(start, format='%Y-%m-%d', utc=True).timestamp()
        end   = pd.to_datetime(end, format='%Y-%m-%d', utc=True).timestamp()
        ss = ','.join([v + ':' +k for k, v in fromToAddrs_chains.items()])
        dd = dict(starttimestamp=start, endtimestamp=end,
                  sourcechain=sourcechain, address=ss, limit=limit)
        param = urlencode(dd, quote_via=quote)
        resp = self._get('BRIDGES', f'/transactions/{bridge_id}', params=param) 
        return pd.DataFrame(resp)