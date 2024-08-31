-- sql1
select """\"", '''\"'''
; select 2; -- sql2
-- sql3
/* multi
line
comment */
insert into t values (1,2,3);
-- sql4
/*! special mysql comment */
update t set c=4 where id=";;";
