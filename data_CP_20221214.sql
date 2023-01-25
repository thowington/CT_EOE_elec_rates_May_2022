
select a.year_charge as year,
a.zipcode,
b.zcta,
b.po_name as municipality,
a.supplier,
sum(a.num_customers) total_customers,
sum(a.overpayment) net_overpayment,
a.edc
--c.median_hh_income,
--d.pct_minority,
--e.pct_low_english
from final_products.rate_comparison a
left join acs.crosswalk b
on trim(a.zipcode) = trim(b.zipcode)
--join acs.income c
--on trim(b.zcta) = trim(c.zcta)
--join acs.majority_minority d
--on trim(b.zcta) = trim(d.zcta)
--join acs.pct_low_english e
--on trim(b.zcta) = trim(b.zcta)
group by a.year_charge, a.zipcode, a.supplier, a.edc, b.zcta, b.po_name
--c.median_hh_income, d.pct_minority, e.pct_low_english
order by a.year_charge, a.zipcode, a.supplier