batting = load 'Batting.csv' using PigStorage(',') as (playerID, yearID, stint, teamID, lgID, G, G_batting, AB, R:int, H, B2, B3, HR, RBI, SB, CS, BB, SO, IBB, HBP, SH, SF, GIDP);
filtered = filter batting by yearID=='1960' AND teamID=='ML1';
grpd = group filtered all;
max = foreach grpd generate MAX(filtered.R);
max_tuple = filter filtered by R==max.$0;
result = foreach max_tuple generate playerID, R;
dump result;
