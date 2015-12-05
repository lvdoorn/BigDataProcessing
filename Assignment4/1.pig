salaries = load 'Salaries.csv' using PigStorage(',') as (year,team,league,id,salary);
filtered = filter salaries by year=='1985'; 
teamsalary = foreach filtered generate team, salary;
teamgroup = group teamsalary by team; 
minsal = foreach teamgroup generate MIN(teamsalary.salary), group;   
result = filter minsal by $0 > 100000; 
endresult = foreach result generate $1;
dump endresult;
