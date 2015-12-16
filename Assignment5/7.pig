REGISTER piggybank.jar;
payroll1 = load 'majorleague-payroll.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage() as (rank:int, team:chararray, total:chararray, avg:chararray, median:chararray, stddev:chararray);
payroll2 = load 'majorleague-payroll2.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage() as (team:chararray, total:chararray, avg:chararray, median:chararray);
baseball = load 'baseball' as (player:chararray, team:chararray, positions:bag{ pos:tuple(posname:chararray) }, stats:map[]);
joined1 = join payroll1 by team, payroll2 by team;
filtered = filter joined1 by payroll1::avg > '$3,000,000' AND payroll2::avg > '$3,000,000';
joined2 = join filtered by payroll1::team, baseball by team;
result = foreach joined2 generate player;
dump result;

