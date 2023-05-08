with tem1 as (
select
    *
from
    bybit_entry
where
    exchangename = 'kucoin'
)
select
    *
from
    kuucoin_fund_fee as t1
left join
    tem1 as t2 on t1.symbol = t2.symbol
where
    t1.value > 0.0001 and
    t1.predictedvalue > 0.0001 and
    t2.symbol is null
;
