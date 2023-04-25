select
    t1.*
    ,case
        when symbolname like '%USDTM' then replace(symbolname,'USDTM','-USDT')
        when symbolname like '%USDM' then replace(symbolname,'USDTM','-USD')
        when symbolname like '%USCDM%' then replace(symbolname,'USCDM','-USCD')
        when symbolname = 'XBTUSDTM' then 'BTC-USDT'
     end as spot_symbol
from
    kuucoin_fund_fee as t1
where
    symbolname not in('XRPUSDM','ETHUSDM','DOTUSDM','XBTUSDM') and
    t1.value >= 0.001
order by
    cast(timepoint as bigint)
;
