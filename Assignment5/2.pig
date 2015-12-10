baseball = load 'baseball' as (player:chararray, team:chararray, positions:bag{ pos:tuple(posname:chararray) }, stats:map[]);
players100 = filter baseball by stats#'games' > 100;
relevantdata = foreach relevantdata generate $0, (int)$1, (int)$2;
hrp = foreach relevantdata generate player, homerun.HomeRunPercentage($2,$1);
result = order hrp by $1 desc;

