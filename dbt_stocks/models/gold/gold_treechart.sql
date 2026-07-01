WITH source AS (
  SELECT
    symbol,
    TRY_CAST(current_price AS DOUBLE) AS current_price_dbl,
    market_timestamp
  FROM {{ ref('silver_clean_stock_quotes') }}
  -- optionally filter invalid rows:
  WHERE TRY_CAST(current_price AS DOUBLE) IS NOT NULL
),

"""Q1. So DOUBLE changes a text value to numeric?

Yes, if the text represents a valid number.
Example:

current_price (TEXT)	TRY_CAST(... AS DOUBLE)
"100"	100.0
"99.75"	99.75
"abc"	NULL

So DOUBLE is simply the target data type (a floating-point number).
  
  
  
  
Q2. Why -> WHERE TRY_CAST(current_price AS DOUBLE) IS NOT NULL

instead of -> WHERE current_price_dbl IS NOT NULL

The answer is SQL execution order.
The query is:
SELECT
    TRY_CAST(current_price AS DOUBLE) AS current_price_dbl
FROM table
WHERE current_price_dbl IS NOT NULL
This does not work in standard SQL because the alias current_price_dbl doesn't exist yet when the WHERE clause is evaluated.

The logical order is:
FROM
WHERE
GROUP BY
HAVING
SELECT
ORDER BY

Notice that WHERE runs before SELECT.

  
Q3. Is TRY_CAST the same as CAST?

Almost. The difference is how they handle invalid data.

CAST
CAST('100' AS DOUBLE)

✅ Returns:
100.0

But
CAST('abc' AS DOUBLE)
❌ Throws an error and the query stops.

TRY_CAST
TRY_CAST('100' AS DOUBLE)

✅ Returns:
100.0
TRY_CAST('abc' AS DOUBLE)

✅ Returns:
NULL
No error.

So TRY_CAST is safer when your data may contain bad values.
Some databases don't have TRY_CAST().
For example:

SQL Server → ✅ TRY_CAST()
Snowflake → ✅ TRY_CAST()
PostgreSQL → ❌ No TRY_CAST()
MySQL → ❌ Doesn't have TRY_CAST() in the same way

In databases without TRY_CAST(), you usually clean or validate the data first."""

  
latest_day AS (
  -- if market_timestamp is epoch seconds (NUMBER/INT):    
  SELECT CAST(TO_TIMESTAMP_LTZ(MAX(market_timestamp)) AS DATE) AS max_day """#rightnow market_timestamp is holding value like this (1719676800) It's called an Epoch timestamp (seconds since January 1, 1970)."""
  FROM source
),

"""What does TO_TIMESTAMP_LTZ() do?

It's a conversion function.
For example, suppose your data is stored as an epoch number: 1719676800

This is just an integer.
When you do:  TO_TIMESTAMP_LTZ(1719676800)

Snowflake converts that integer into a proper TIMESTAMP_LTZ value.
For example:
1719676800
        │
        ▼
2026-06-30 00:00:00 +05:30   (displayed in your session's timezone)

Now it's a timestamp that SQL can work with.

Then why do we cast it to DATE?
CAST(TO_TIMESTAMP_LTZ(market_timestamp) AS DATE)

This happens in two steps:
  
Step 1
TO_TIMESTAMP_LTZ(1719676800)
↓
2026-06-30 09:15:22

Step 2
CAST(... AS DATE)
↓
2026-06-30

The CAST(... AS DATE) removes the time portion, not the timezone."""

  
latest_prices AS (
  SELECT
    symbol,
    AVG(current_price_dbl) AS avg_price
  FROM source
  JOIN latest_day ld
    ON CAST(TO_TIMESTAMP_LTZ(market_timestamp) AS DATE) = ld.max_day
  GROUP BY symbol
),

all_time_volatility AS (
  SELECT
    symbol,
    STDDEV_POP(current_price_dbl) AS volatility,             
    CASE
      WHEN AVG(current_price_dbl) = 0 THEN NULL
      ELSE STDDEV_POP(current_price_dbl) / NULLIF(AVG(current_price_dbl), 0)  """standard deviation / average price"""
    END AS relative_volatility
  FROM source
  GROUP BY symbol
)

"""Q1. What does NULLIF() do?

NULLIF(a, b) means: If a equals b, return NULL; otherwise, return a.

Examples: NULLIF(100, 0)
returns
100
because 100 ≠ 0.

NULLIF(0, 0)
returns 
NULL
because the two values are equal.
  
Applying it to your query
If the average is: 100
then
NULLIF(100, 0)
returns
100
Expression becomes:
STDDEV_POP(...) / 100

  
If the average is: 0
then
NULLIF(0, 0)
returns
NULL
Expression becomes:
STDDEV_POP(...) / NULL
  
And in SQL: Any number divided by NULL results in NULL, not an error.

But your query also has this:
CASE
    WHEN AVG(current_price_dbl) = 0 THEN NULL
    ELSE STDDEV_POP(current_price_dbl) /
         NULLIF(AVG(current_price_dbl), 0)
END

You might wonder: If we're already checking AVG(...) = 0, why use NULLIF() too?

You're right to notice that.
Strictly speaking, the CASE statement already prevents division by zero, so NULLIF() is redundant here.

The code could simply be:
CASE
    WHEN AVG(current_price_dbl) = 0 THEN NULL
    ELSE STDDEV_POP(current_price_dbl) / AVG(current_price_dbl)
END

Many developers still include NULLIF() as an extra safety measure or defensive programming habit. That way, even if someone later removes or changes the CASE,
the division still won't fail when the average is 0. """"
  
  
  
  
  
Q2. STDDEV_POP() calculates the population standard deviation.

In simple words: bIt measures how much the values vary (or fluctuate) from their average.

For stock prices, this means:
Small standard deviation → Price is stable.
Large standard deviation → Price moves up and down a lot (more volatile).
  
Example 1: Stable stock
Prices:
100
101
99
100
100

Average:
100
The prices are very close to the average.

STDDEV_POP(price)
might return
0.63
Very small volatility.

Example 2: Volatile stock
Prices:
50
150
80
130
90

Average:
100
The prices are spread out.
STDDEV_POP(price)
might return
36.8
Much larger volatility.

  
In your query
SELECT
    symbol,
    STDDEV_POP(current_price_dbl) AS volatility
FROM source
GROUP BY symbol;

Suppose your data is:
symbol	price
AAPL	100
AAPL	102
AAPL	101
AAPL	99

For AAPL:
Average:
100.5

Standard deviation:
≈ 1.12

Result:
symbol	volatility
AAPL	1.12

  
Why is it called STDDEV_POP?
There are two versions of standard deviation.

1. STDDEV_POP()
Uses the entire population.
Formula divides by N.
If you have all your stock price records, this is usually the appropriate choice.

2. STDDEV_SAMP()
Uses a sample of the population.
Formula divides by N − 1.
This is used in statistics when your data is only a sample of a larger population.
  
Which one should you use?
For a data warehouse where you have all the prices you want to analyze, STDDEV_POP() is a sensible choice because you're treating the dataset as the full population for your analysis.

Then this line:
STDDEV_POP(current_price_dbl) /
NULLIF(AVG(current_price_dbl), 0)

calculates relative volatility.
Example:
Prices:
100
110
90
100

Average:
100

Standard deviation:
7.07

Relative volatility:
7.07 / 100
=
0.0707
=
7.07%

This tells you the price fluctuates by about 7% of its average price, making it easier to compare stocks with very different price levels (for example, a ₹50 stock and a ₹5,000 stock)."""


  
SELECT
  lp.symbol,
  lp.avg_price,
  v.volatility,
  v.relative_volatility
FROM latest_prices lp
JOIN all_time_volatility v ON lp.symbol = v.symbol
ORDER BY lp.symbol
