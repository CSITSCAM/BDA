divs = load 'input/NYSE.txt' as (exchange, stock_symbol, date, dividends);
daily = load 'input/NYSE_daily.txt';
jnd = join divs by stock_symbol, daily by $1;
dump jnd;
