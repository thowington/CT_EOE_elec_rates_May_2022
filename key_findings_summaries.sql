
-- number of supplier-customers from EDC data (end of each year)
select year, sum(num_custs_supplier)
from q38.participation_data
group by year
union
select 'avg' as year, sum(num_custs_supplier)*1.0/5
from q38.participation_data
order by year;

-- estimate of supplier customers using suppler bills / 12 per year
select year_charge, sum(num_customers) *1.0 / 12
from q6.all
group by year_charge
union
select 'avg' as year_charge, sum(num_customers) *1.0 / 60
from q6.all
order by year_charge