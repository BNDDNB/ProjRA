#Quant Practice

#utilize quantopian API to import fundamental data and 
#filter for whether it is primary share

import doctest #maynot use this for now
from quantopian.pipeline.data import Fundamentals
from quantopian.pipeline.filters.fundamentals import IsPrimaryShare

'''
checking if instrument is primary share 
$$prim_share = IsPrimaryShare()

checking if it's common gives the following
$$com_share = Fundamentals.security_type.latest.eq('ST00000001')

checking if its depositary receipt
a depositary receipt is a negotiable instrument issued by a bank 
to represent a foreign company's publicly traded securities 
$$ndep_receipt = ~Fundamentals.is_depositary_receipt.latest

chcking if it is OTC
$$nOTC = ~Fundamentals.exchange_id.latest.startswith('OTC')

Not when-issued equity
when-issued is a transaction that is made conditionally because 
a security has been authorized but not yet issued.
$$nWI = ~Fundamentals.symbol.latest.endswith('.WI')

not a limited partnership 
$$nLP = ~Fundamentals.standard_name.latest.matches('.* L[. ]?P.?$')

Doesn't have a company reference entry indicating its a LP
This per Q, states that the Limited Partnership was only given when
it is based on fundamentals from morningstar balance_sheet statements. 
$$nLPBS = Fundamentals.limited_partnership.latest.isnull()

This is not an ETF
$$have_market_cap = Fundamentals.market_cap.latest.notnull()

if any that fits the above criteria, then it is tradable
tradable = (...&&...&)

above are all about filters on the securities, now to apply all the
filters to the whole universe of data will give a workable base
where the avg$vol is a factor rep entire uni without any parameter
univs = AverageDollarVolume(window_length = 10, mask = tradable).percentile_between(70,100)

there are Q1500US and Q500US that can be used for the built-in
pipeline that is already provided in the library

'''

