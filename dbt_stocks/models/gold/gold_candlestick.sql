with enriched as (
    select
        symbol,
        cast(market_timestamp as date) as trade_date,
        day_low,
        day_high,
        current_price,
        first_value(current_price) over (
            partition by symbol, cast(market_timestamp as date)
            order by market_timestamp
        ) as candle_open,
        last_value(current_price) over (
            partition by symbol, cast(market_timestamp as date)
            order by market_timestamp
            rows between unbounded preceding and unbounded following
        ) as candle_close
    from {{ ref('silver_clean_stock_quotes') }}
),

""" Q1) FIRST_VALUE(current_price)
    
Looks at the first row  09:15 → 100
So every row receives  100

Result
Time	Price	candle_open
09:15	100	    100
09:30	103     100
10:00	108	    100


Q2) LAST_VALUE(current_price)   
This does the opposite.
It returns the last price of the day.
    
Look at the last row   10:00	108
So every row receives 108

So the output becomes
Time	Price	candle_close
09:15	100  	108
09:30	103  	108
10:00	108  	108
    

    
Q3) Why do we need?
ROWS BETWEEN UNBOUNDED PRECEDING
         AND UNBOUNDED FOLLOWING

This is important.
By default, LAST_VALUE() only looks up to the current row, not the entire partition.

Without this clause:
Time	Price	LAST_VALUE
09:15	100	    100
09:30	103  	103
10:00	108  	108
It would just return the current row's price.

Adding: ROWS BETWEEN UNBOUNDED PRECEDING
         AND UNBOUNDED FOLLOWING

tells SQL: Consider the entire partition (all rows for this symbol on this day), from the first row to the last row.

Now it correctly returns the day's closing price for every row:
Time	Price	candle_close
09:15	100	    108
09:30	103     108
10:00	108	    108"""

candles as (
    select
        symbol,
        trade_date as candle_time,
        min(day_low) as candle_low,
        max(day_high) as candle_high,
        any_value(candle_open) as candle_open,
        any_value(candle_close) as candle_close,
        avg(current_price) as trend_line
    from enriched
    group by symbol, trade_date
),

ranked as (
    select
        c.*,
        row_number() over (
            partition by symbol
            order by candle_time desc
        ) as rn
    from candles c
)

select
    symbol,
    candle_time,
    candle_low,
    candle_high,
    candle_open,
    candle_close,
    trend_line
from ranked
where rn <= 12
order by symbol, candle_time
