REGISTER piggybank.jar;
payroll = load 'majorleague-payroll.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage() as (rank:int, team:chararray, total:chararray, avg:chararray, median:chararray, stddev:chararray);
baseball = load 'baseball' as (player:chararray, team:chararray, positions:bag{ pos:tuple(posname:chararray) }, stats:map[]);
highpaying = filter payroll by avg > '$3,000,000';
joined = join highpaying by team, baseball by team;
result = foreach joined generate player;
dump result;
