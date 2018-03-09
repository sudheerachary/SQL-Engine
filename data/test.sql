select * from table1
select * from table1, table2
select * from table1, table2, table3
select A from table1
select A, D from table1, table2
select A, B from table1, table2
select          * from    table1   where A      = 1 and B =        2
select * from table1 where ((A = 1 and B = 2) or (C = 3 and A = 2))
select A from table1 where A = 1
select A, B from table1 where (A = B and (A = 1 or B = 2))
select B, D from table1, table2 where A = B and C = D
select A, table1.B, table2.D from table1, table2, table3 where table1.B = table2.B and table2.D = table3.D
select A, table1.B, table2.D from table1, table2, table3 where ((table1.B = table2.B and table2.D = table3.D) or A = 1)