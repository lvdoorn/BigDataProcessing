REGISTER pitcher.jar;
baseball = load 'baseball' as (player:chararray, team:chararray, positions:bag{ pos:tuple(posname:chararray) }, stats:map[int]);
sof = foreach baseball generate player, positions, pitcher.StrikeOutFraction(positions, stats) as fraction;
grpd = group sof all;
avg = foreach grpd generate AVG($1.$2) as average;
data_with_average = foreach sof generate player, positions, fraction, avg.average as average;
goodpitchers = filter data_with_average by pitcher.OnlyGoodPitchers(player, positions, fraction, average);
result = foreach goodpitchers generate player;
dump result;
