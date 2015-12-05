batting = load 'Batting.csv' using PigStorage(',') as (playerID, yearID, stint, teamID, lgID, G, G_batting, AB, R:int, H, B2, B3, HR, RBI, SB, CS, BB, SO, IBB, HBP, SH, SF, GIDP);
salaries = load 'Salaries.csv' using PigStorage(',') as (year,team,league,id,salary);
trimmed_batting = foreach batting generate playerID, HR;
trimmed_salaries = foreach salaries generate id, salary, year;
joined = join trimmed_batting by playerID, trimmed_salaries by id;
filtered = filter joined by HR > 50 AND salary > 500000 AND year=='2001';
result = foreach filtered generate playerID, HR, salary;
dump result;
