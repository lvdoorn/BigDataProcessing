salaries = load 'Salaries.csv' using PigStorage(',') as (year,team,league,id,salary);
filtered = filter salaries by year=='1998';
teamsalary = foreach filtered generate team, salary;
teamgroup = group teamsalary by team;
result = foreach teamgroup generate group, AVG(teamsalary.salary);
dump result;
