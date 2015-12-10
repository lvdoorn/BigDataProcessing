baseball = load 'baseball' as (player:chararray, team:chararray, positions:bag{ pos:tuple(posname:chararray) }, stats:map[]);
playerpos = foreach baseball generate player, positions;
flatplayerpos = foreach playerpos generate player, FLATTEN(positions);
distflatplayerpos = distinct flatplayerpos;
grpd = group distflatplayerpos by $1;
result = foreach grpd generate group, COUNT($1);
dump result;
