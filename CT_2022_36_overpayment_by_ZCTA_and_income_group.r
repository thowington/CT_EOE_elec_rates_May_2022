# overpayment by rate by income group and  ZCTA ----


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

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = "5432",
  user = user1,
  password = password1,
  dbname = "ct_2022"
)



# total overpayment in zctas median HHI < 50,000 ----
res_overpay_income <- dbSendQuery(con,
                               "select '2017' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2017
where zcta in (select zcta from acs.income
			  where median_hh_income < 50000)
union
select '2018' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2018
where zcta in (select zcta from acs.income
			  where median_hh_income < 50000)
union
select '2019' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2019
where zcta in (select zcta from acs.income
			  where median_hh_income < 50000)
union
select '2020' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2020
where zcta in (select zcta from acs.income
			  where median_hh_income < 50000)
union
select '2021' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2021
where zcta in (select zcta from acs.income
			  where median_hh_income < 50000)
order by year")
overpayment_inc_lt_50000 <- dbFetch(res_overpay_income)
overpayment_inc_lt_50000






# total overpayment in zctas w minoritites > 35%
res_overpay_minority <- dbSendQuery(con,
                                  "select '2017' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2017
where zcta in (select zcta from acs.majority_minority
			  where pct_minority > 0.35)
union
select '2018' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2018
where zcta in (select zcta from acs.majority_minority
			  where pct_minority > 0.35)
union
select '2019' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2019
where zcta in (select zcta from acs.majority_minority
			  where pct_minority > 0.35)
union
select '2020' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2020
where zcta in (select zcta from acs.majority_minority
			  where pct_minority > 0.35)
union
select '2021' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2021
where zcta in (select zcta from acs.majority_minority
			  where pct_minority > 0.35)
order by year")
overpayment_min_gt_35pct <- dbFetch(res_overpay_minority)
overpayment_min_gt_35pct




# total overpayment in zctas w > 10% hh with low english
res_overpay_low_eng <- dbSendQuery(con,
                                    "select '2017' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2017
where zcta in (select zcta from acs.pct_low_english
			  where pct_low_english > 0.10)
union
select '2018' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2018
where zcta in (select zcta from acs.pct_low_english
			  where pct_low_english > 0.10)
union
select '2019' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2019
where zcta in (select zcta from acs.pct_low_english
			  where pct_low_english > 0.10)
union
select '2020' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2020
where zcta in (select zcta from acs.pct_low_english
			  where pct_low_english > 0.10)
union
select '2021' as Year, sum(net_overpayment)
from final_products.payment_comparison_by_zcta_2021
where zcta in (select zcta from acs.pct_low_english
			  where pct_low_english > 0.10)
order by year")
overpayment_low_eng_gt_10pct <- dbFetch(res_overpay_low_eng)
overpayment_low_eng_gt_10pct

overpayments <- rbind(c("Median Income Less than $50,000",""),
                      overpayment_inc_lt_50000,
                      c("Greater than 35% Minorities",""),
                      overpayment_min_gt_35pct,
                      c("Greater than 10% Low English Proficiency",""),
                      overpayment_low_eng_gt_10pct )
overpayments

filename = paste0(project_dir, "/output/overpayment_by_demographics.csv")
write_csv(overpayments, filename)
