-- origin of revenues by mediaan_hh_income

select a.zcta, a.geographic_name, a.median_hh_income
,b.gross_overpayment, c.participation_rate
from acs.income a
join final_products.payment_comparison_by_zcta_2021 b
on a.zcta = b.zcta
join final_products.participation_rate_by_year_zcta c
on a.zcta = c.zcta
where c.year = '2021'
order by a.median_hh_income asc