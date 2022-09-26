library(RPostgres)
library(odbc)
library(dplyr)
library(readxl)
library(ConfigParser)
library(stringr)
library(readr)

config_file<- "C:/Users/thowi/Documents/consulting_work/DO_NOT_SHARE/CT_2022_config_file.ini"
config_parameters <- ConfigParser$new()
perms <- config_parameters$read(config_file)
user1 <- perms$get("user")
password1 <- perms$get("password")

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = "5432",
  user = user1,
  password = password1,
  dbname = "ct_2022"
)

# find weighted average rate for all customers
dbSendQuery(con, "drop table if exists analysis.wtd_avg_rates_all;")



dbSendQuery(con, "select year_charge, billed_rate, sum(totalkwh) kwh_all
                    into analysis.wtd_avg_rates_all
                    from q6.all
                    group by year_charge, billed_rate
                    order by year_charge, billed_rate;")

# find weighted avg rates for customers choosing "green" alternative
dbSendQuery(con, "drop table if exists analysis.wtd_avg_rates_green;")

dbSendQuery(con, "select year_charge, billed_rate, sum(totalkwh) kwh_green
        into analysis.wtd_avg_rates_green 
        from q9.all
        group by year_charge, billed_rate
        order by year_charge, billed_rate;")


# join tables and find nongreen usage
dbSendQuery(con, "drop table if exists analysis.wtd_avg_rates_consolidated;")

dbSendQuery(con, "select a.year_charge as all_year, 
a.billed_rate as all_rate, 
coalesce(a.kwh_all, 0) as all_kwh,
b.year_charge as green_year, 
b.billed_rate as green_rate, 
coalesce(b.kwh_green,0) as green_kwh
into analysis.wtd_avg_rates_consolidated
from analysis.wtd_avg_rates_all a
full join analysis.wtd_avg_rates_green b
on a.year_charge = b.year_charge
and a.billed_rate = b.billed_rate;")


dbSendQuery(con, "alter table analysis.wtd_avg_rates_consolidated
add nongreen_kwh numeric,
add year char(4),
add rate numeric(6,5);")


dbSendQuery(con, "update analysis.wtd_avg_rates_consolidated
set nongreen_kwh = all_kwh - green_kwh;")

# remove negative amounts
dbSendQuery(con, "update analysis.wtd_avg_rates_consolidated
set nongreen_kwh = 0
where nongreen_kwh <0;")

# fill in the blanks where there was no join
dbSendQuery(con, "update analysis.wtd_avg_rates_consolidated
set year = coalesce(all_year, green_year);")

dbSendQuery(con, "update analysis.wtd_avg_rates_consolidated
set rate = coalesce(all_rate, green_rate);")



dbSendQuery(con, "drop table if exists final_products.wtd_avg_rates;")

dbSendQuery(con, "select year, 
sum(rate*all_kwh)/sum(all_kwh) as wtd_avg_all,
sum(rate*nongreen_kwh)/sum(nongreen_kwh) as wtd_avg_nongreen,
sum(rate*green_kwh)/sum(green_kwh) as wtd_avg_green
into final_products.wtd_avg_rates
from analysis.wtd_avg_rates_consolidated
group by year
order by year;")

res1 <- dbSendQuery(con, "select * from final_products.wtd_avg_rates;")
result <- dbFetch(res1, n=-1)

filename = "C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/output/wtd_avg_rates.csv"
write_csv(result, filename)

# find green versus nongreen usage

dbSendQuery(con, "drop table if exists final_products.usage_by_green_nongreen;")

dbSendQuery(con, "select year, sum(all_kwh) as total_kwh,
sum(green_kwh) as green_kwh,
sum(nongreen_kwh) as nongreen_kwh,
sum(green_kwh)/sum(all_kwh) as green_percentage
into final_products.usage_by_green_nongreen
from analysis.wtd_avg_rates_consolidated
group by year
order by year;")

res2 <- dbSendQuery(con, "select * from final_products.usage_by_green_nongreen")
result2 <- dbFetch(res2, n=-1)

filename = "C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/output/usage_green_v_nongreen.csv"
write_csv(result2, filename)



print("finished creating weighted averages.")
