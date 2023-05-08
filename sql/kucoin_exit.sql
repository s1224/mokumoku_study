with tem1 as (
select
    t1.*
    ,t2.fundingrate
    ,case
        when cast(t2.fundingrate as float) < 0 then 1
        else 0
     end exit_timing
from
    bybit_entry as t1
inner join
    bybit_dervatives as t2 on t1.symbol = t2.symbol
where
    t1.exchangename = 'kucoin' and
    t1.featureorspot = 'feature'
)
select
    *
from
    tem1
where
    exit_flag = '0'
    and
    exit_timing = 1
;
