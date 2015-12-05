salaries = load 'Salaries.csv' using PigStorage(',') as (year,team,league,id,salary);
filtered = filter salaries by year=='1999';
teamleague = foreach filtered generate team, league;
unique_teamleague = distinct teamleague;
grouped_teamleague = group unique_teamleague by league;
countteam = foreach grouped_teamleague generate group, COUNT(unique_teamleague.team);
dump countteam;
