select b.zipcode, b.zcta,
c.count_households,
f.total_population,
c.median_hh_income,
d.pct_minority,
e.pct_low_english
from acs.crosswalk b
join acs.income c
on b.zcta = c.zcta
join acs.majority_minority d
on b.zcta = d.zcta
join acs.pct_low_english e
on b.zcta = e.zcta
join acs.total_population f
on b.zcta = f.zcta