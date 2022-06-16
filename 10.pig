daily = load 'input/NYSE_daily.txt';
calcs = foreach daily generate $7 / 1000000, $3 * 100.0, SUBSTRING($0, 0, 1), $6 - $3;
dump calcs;
