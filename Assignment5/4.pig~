baseball = load 'baseball' as (player:chararray, team:chararray, positions:bag{ pos:tuple(posname:chararray) }, stats:map[]);
positions = foreach baseball generate FLATTEN(positions);
result = distinct positions;

