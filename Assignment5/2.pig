REGISTER pitcher.jar;
baseball = load 'baseball' as (player:chararray, team:chararray, positions:bag{ pos:tuple(posname:chararray) }, stats:map[]);
players100 = filter baseball by stats#'games' > 100;
relevantdata = foreach players100 generate $0, (int)stats#'games', (int)stats#'home_runs';
hrp = foreach relevantdata generate player, pitcher.HomeRunPercentage($2,$1);
result = order hrp by $1 desc;
dump result;
