daily = load 'input/NYSE_daily.txt';
fltrd = filter daily by $1 == 'CRT' AND $8 > 30.0;
dump fltrd;
