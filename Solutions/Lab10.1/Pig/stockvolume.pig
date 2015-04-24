register stockudfs.jar;

stockdata = LOAD 'stocksA' using PigStorage(',') AS 
	(exchange:chararray, 
	symbol:chararray, 
	date:chararray, 
	open:float, 
	high:float, 
	low:float, 
	close:float, 
	volume:int);
stocks_all = FOREACH stockdata GENERATE symbol, date, close, volume;
stocks_filter = FILTER stocks_all BY symbol == '$symbol';
stocks_sorted = ORDER stocks_filter BY date ASC;
obv_result = FOREACH stocks_sorted GENERATE symbol, date, stockudfs.OnBalanceVolume(volume, close) AS obv;
dump obv_result;
