with
    ha as(
        select
            za,
            max(ab) bb,min(ab) cb,
            max(db) eb,min(db) fb,
            max(gb) hb,min(gb) ib,
            max(jb)*1600*0.278 kb
        from(
                select za,lb,
                       cast(if(json_extract(tb,"$.foo1")=0 or json_extract(tb,"$.foo1") like "%E%",null,json_extract(tb,"$.foo1")) as double) ab,
                       cast(if(json_extract(tb,"$.foo2")=0 or json_extract(tb,"$.foo2") like "%E%",null,json_extract(tb,"$.foo2")) as double) db,
                       first_value(cast(if(json_extract(tb,"$.bar")=0 or json_extract(tb,"$.bar") like "%E%",null,
                                           json_extract(tb,"$.bar")) as double)) over (partition by za order by lb desc) jb,
                        cast(if(json_extract(tb,"$.foobar")=0 or json_extract(tb,"$.foobar") like "%E%",null,json_extract(tb,"$.foobar")) as double) gb
                from ob
                where (za ="hai2342023_1_1s" or za ="hai2342123_1s"
                    or za ="hai2342223_1_1s" or za ="hai2342323_1s"
                    or za ="hai2342423_1_1s" or za ="hai2342523_1s")
                  and lb>="2023-06-10 00:00:00" and lb<"2023-06-10 06:00:00"
            )pb
        group by za
    ),
    qb as(
        select
            za,MAX(hb) hb,MAX(eb) eb,MAX(bb) bb
        from rb
        where lb="2023-06-09"
        group by za
    )select
         "2023-06-10",
         ha.za,
         cast(nvl(if(ha.hb-qb.hb>0,ha.hb-qb.hb,ha.hb-ha.ib),0) as decimal(10,4)),
         nvl(ha.hb,0),
         cast(nvl(if(ha.eb-qb.eb>0,ha.eb-qb.eb,ha.eb-ha.fb)*277.78,0) as decimal(10,4)),
         nvl(ha.eb,0),
         cast(nvl(if(ha.bb-qb.bb>0,ha.bb-qb.bb,ha.bb-ha.cb)*277.78,0) as decimal(10,4)),
         nvl(ha.bb,0),
         cast(nvl(ha.kb,0) as decimal(20,4)),
         "day"
from ha left join qb
                   on ha.za=qb.za;
