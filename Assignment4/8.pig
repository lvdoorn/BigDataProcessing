REGISTER homerun.jar;
batting = load 'Batting.csv' using PigStorage(',') as (playerID, yearID, stint, teamID, lgID, G:int, G_batting, AB, R, H, B2, B3, HR:int, RBI, SB, CS, BB, SO, IBB, HBP, SH, SF, GIDP);
percentages = foreach batting generate playerID, homerun.HomeRunPercentage(HR, G) as hrp;
ordered_percentages = order percentages by hrp desc;
top10 = limit ordered_percentages 10;
dump top10;
