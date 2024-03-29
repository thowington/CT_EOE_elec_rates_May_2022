# overpayment by top and bottom by demographics

library(RPostgres)
library(odbc)
library(dplyr)
library(readxl)
library(ConfigParser)
library(stringr)
library(ggplot2)
library(reshape2)

config_file<- "C:/Users/thowi/Documents/consulting_work/DO_NOT_SHARE/CT_2022_config_file.ini"
config_parameters <- ConfigParser$new()
perms <- config_parameters$read(config_file)
user1 <- perms$get("user")
password1 <- perms$get("password")
project_dir <- perms$get("project_dir")
dbase <- perms$get("this_database")

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = "5432",
  user = user1,
  password = password1,
  dbname = dbase
)

# overpayment by percent minority ----
res1 <- dbSendQuery(con, "select * from (
select a.zcta, a.pct_minority, net_overpayment, net_overpayment * 1.0 / totalkwh as overpayment_kwh
from acs.majority_minority a
join final_products.overpayment_per_kwh_zcta_2021 b
on a.zcta = b.zcta
where a.pct_minority is not NULL
order by pct_minority asc
limit 10
	) as aa
union
select * from (
select a.zcta, a.pct_minority,  net_overpayment, net_overpayment * 1.0 / totalkwh as overpayment_kwh
from acs.majority_minority a
join final_products.overpayment_per_kwh_zcta_2021 b
on a.zcta = b.zcta
where a.pct_minority is not NULL
order by pct_minority desc
limit 10
	) as bb
order by pct_minority desc")

overpay_by_topbottom_min_pct <- dbFetch(res1)
overpay_by_topbottom_min_pct
filename = paste0(project_dir, "/output/overpay_by_topbottom_min_pct.csv")
write.csv(overpay_by_topbottom_min_pct, filename, row.names = FALSE)


# overpayment by median income ----
res2 <- dbSendQuery(con, "select * from (
select a.zcta, a.median_hh_income, net_overpayment, net_overpayment * 1.0 / totalkwh as overpayment_kwh
from acs.income a
join final_products.overpayment_per_kwh_zcta_2021 b
on a.zcta = b.zcta
where a.median_hh_income is not NULL
order by a.median_hh_income desc
limit 10
	) as aa
union
select * from (
select a.zcta, a.median_hh_income,  net_overpayment, net_overpayment * 1.0 / totalkwh as overpayment_kwh
from acs.income a
join final_products.overpayment_per_kwh_zcta_2021 b
on a.zcta = b.zcta
where a.median_hh_income is not NULL
order by a.median_hh_income asc
limit 10
	) as bb
order by median_hh_income desc")

overpay_by_topbottom_income <- dbFetch(res2)
overpay_by_topbottom_income
filename = paste0(project_dir, "/output/overpay_by_topbottom_income.csv")
write.csv(overpay_by_topbottom_income, filename, row.names = FALSE)


# overpayment by pct low english ----
res3 <- dbSendQuery(con, "select * from (
select a.zcta, a.pct_low_english, net_overpayment, net_overpayment * 1.0 / totalkwh as overpayment_kwh
from acs.pct_low_english a
join final_products.overpayment_per_kwh_zcta_2021 b
on a.zcta = b.zcta
where a.pct_low_english is not NULL
order by a.pct_low_english asc
limit 10
	) as aa
union
select * from (
select a.zcta, a.pct_low_english,  net_overpayment, net_overpayment * 1.0 / totalkwh as overpayment_kwh
from acs.pct_low_english a
join final_products.overpayment_per_kwh_zcta_2021 b
on a.zcta = b.zcta
where a.pct_low_english is not NULL
order by a.pct_low_english desc
limit 10
	) as bb
order by pct_low_english asc")
overpay_by_topbottom_english <- dbFetch(res3)
overpay_by_topbottom_english
filename = paste0(project_dir, "/output/overpay_by_topbottom_english.csv")
write.csv(overpay_by_topbottom_english, filename, row.names = FALSE)


# savings / overpayment by percent minority with per_kwh premium----
res1 <- dbSendQuery(con, "select * from (
select a.zcta, a.pct_minority, 
bills_with_savings,
savings_per_kwh,
savings_per_bill,
bills_with_overpay,
overpayment_per_kwh,
overpayment_per_bill
from acs.majority_minority a
join final_products.save_overpay_by_zcta_2021 b
on a.zcta = b.zcta
where a.pct_minority is not NULL
order by pct_minority asc
limit 15
	) as aa
union
select * from (
select a.zcta, a.pct_minority, 
bills_with_savings,
savings_per_kwh,
savings_per_bill,
bills_with_overpay,
overpayment_per_kwh,
overpayment_per_bill
from acs.majority_minority a
join final_products.save_overpay_by_zcta_2021 b
on a.zcta = b.zcta
where a.pct_minority is not NULL
order by pct_minority desc
limit 10
	) as bb
order by pct_minority desc")

save_overpay_by_topbottom_min_pct <- dbFetch(res1)
save_overpay_by_topbottom_min_pct
filename = paste0(project_dir, "/output/save_overpay_by_topbottom_min_pct.csv")
write.csv(save_overpay_by_topbottom_min_pct, filename, row.names = FALSE)


# savings / overpayment by median_hh_income with per_kwh premium----
res1 <- dbSendQuery(con, "select * from (
select a.zcta, a.median_hh_income, 
bills_with_savings,
savings_per_kwh,
savings_per_bill,
bills_with_overpay,
overpayment_per_kwh,
overpayment_per_bill
from acs.income a
join final_products.save_overpay_by_zcta_2021 b
on a.zcta = b.zcta
where a.median_hh_income is not NULL
order by a.median_hh_income desc
limit 10
	) as aa
union
select * from (
select a.zcta, a.median_hh_income, 
bills_with_savings,
savings_per_kwh,
savings_per_bill,
bills_with_overpay,
overpayment_per_kwh,
overpayment_per_bill
from acs.income a
join final_products.save_overpay_by_zcta_2021 b
on a.zcta = b.zcta
where a.median_hh_income is not NULL
order by a.median_hh_income asc
limit 10
	) as bb
order by median_hh_income desc")

save_overpay_by_topbottom_income <- dbFetch(res1)
save_overpay_by_topbottom_income
filename = paste0(project_dir, "/output/save_overpay_by_topbottom_income.csv")
write.csv(save_overpay_by_topbottom_income, filename, row.names = FALSE)


# savings / overpayment by pct_low_english with per_kwh premium----
res1 <- dbSendQuery(con, "select * from (
select a.zcta, a.pct_low_english, 
bills_with_savings,
savings_per_kwh,
savings_per_bill,
bills_with_overpay,
overpayment_per_kwh,
overpayment_per_bill
from acs.pct_low_english a
join final_products.save_overpay_by_zcta_2021 b
on a.zcta = b.zcta
where a.pct_low_english is not NULL
order by a.pct_low_english asc
limit 15
	) as aa
union
select * from (
select a.zcta, a.pct_low_english, 
bills_with_savings,
savings_per_kwh,
savings_per_bill,
bills_with_overpay,
overpayment_per_kwh,
overpayment_per_bill
from acs.pct_low_english a
join final_products.save_overpay_by_zcta_2021 b
on a.zcta = b.zcta
where a.pct_low_english is not NULL
order by a.pct_low_english desc
limit 10
	) as bb
order by pct_low_english asc")

save_overpay_by_topbottom_english <- dbFetch(res1)
save_overpay_by_topbottom_english
filename = paste0(project_dir, "/output/save_overpay_by_topbottom_english.csv")
write.csv(save_overpay_by_topbottom_english, filename, row.names = FALSE)


print("finished this script.")