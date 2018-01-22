#Quant Practice

#utilize quantopian API to import fundamental data and 
#filter for whether it is primary share

import doctest #maynot use this for now
from quantopian.pipeline.data import Fundamentals
from quantopian.pipeline.filters.fundamentals import IsPrimaryShare
from quantopian.pipeline.filters.fundamentals import Q1500US

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
pipeline that is already provided in the library, these universe are
meant to be used as masks or filters


all above code that starts with $$ was essentially a way of filtering out
the wanted type of instruments characteristics

now if we were to directly use in-built factors, we would get the following
'''
base_u = Q1500US() #this is a mask or filter condition, does not return all of instruments

#some analysis

mean_10 = SimpleMovingAnverage(inputs = [USEquityPricing.close], window_length = 10, mask = base_u)
mean_30 = SimpleMovingAnverage(inputs = [USEquityPricing.close], window_length = 30, mask = base_u)

#then based on this, create %diff (whats the diff comparing 30-10 or 10-30)
pct_dif = (mean_30 - mean_10) / mean_10
#this create filters for target instruments when calc in pipe
longs = pct_dif.bottom(75) 
shorts = pct_dif.top(75)

#putting above all together
def make_pipeline():
	base_u = Q1500US()
	mean_10 = SimpleMovingAnverage(inputs = [USEquityPricing.close], window_length = 10, mask = base_u)
	mean_30 = SimpleMovingAnverage(inputs = [USEquityPricing.close], window_length = 30, mask = base_u)
	pct_dif = (mean_30 - mean_10) / mean_10
	longs = pct_dif.bottom(75) 
	shorts = pct_dif.top(75)
	ls_screen = (longs|shorts)
	return Pipeline(
		columns = {
		'longs':longs
		'shorts':shorts
		},
		screen = securities_to_trade
	)

result = run_pipeline(make_pipeline(),start_time, end_time)

