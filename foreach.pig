dividends = load 'input/NYSE.txt' as (exchange, symbol, date, dividend);
grouped = group dividends by symbol;
avg = foreach grouped generate group, AVG(dividends.dividend);
store avg into 'average_dividend_op.txt';
