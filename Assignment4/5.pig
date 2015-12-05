batting = load 'Batting.csv' using PigStorage(',') as (playerID, yearID, stint, teamID, lgID, G, G_batting:int, AB, R, H, B2, B3, HR, RBI, SB, CS, BB, SO, IBB, HBP, SH, SF, GIDP);
filtered = filter batting by yearID=='1980';
playerbatted = foreach filtered generate playerID, G_batting;
grpd = group playerbatted all;
max = foreach grpd generate MAX(playerbatted.G_batting);
max_tuple = filter playerbatted by G_batting == max.$0;
dump max_tuple;
