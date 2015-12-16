baseball = load 'baseball' as (player:chararray, team:chararray, positions:bag{ pos:tuple(posname:chararray) }, stats:map[]);
teamgames = foreach baseball generate team, stats#'games';
grpd = group teamgames by team;
avg = foreach grpd generate group, AVG(teamgames.$1);
dump avg;
