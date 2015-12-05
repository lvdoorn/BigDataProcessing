batting = load 'Batting.csv' using PigStorage(',') as (playerID, yearID, stint, teamID, lgID, G, G_batting, AB, R, H:int, B2, B3, HR, RBI, SB, CS, BB, SO, IBB, HBP, SH, SF, GIDP);
filtered = filter batting by yearID=='1988';
playerhits = foreach filtered generate playerID, H;
orderedplayerhits = order playerhits by H desc;
top10 = limit orderedplayerhits 10;
dump top10;
