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
project_dir <- perms$get("project_dir")

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = "5432",
  user = user1,
  password = password1,
  dbname = "ct_2022"
)

res1 <- dbSendQuery(con, "select supplier, 
	concat(year_charge,month_charge) as date_concat,
	year_charge, 
	month_charge, 
	billed_rate, 
	zipcode,
	num_customers,
	totalkwh,
	edc
from q6.all where edc = 'EV'")
res_ev <- dbFetch(res1)
str(res_ev)


res2 <- dbSendQuery(con, "select * from edc_data.edc_rates")
edc_rates <- dbFetch(res2)
str(edc_rates)

# convert cents to dollars
edc_rates$eversource_rate <- edc_rates$eversource_rate/100.0
edc_rates$ui_rate <- edc_rates$ui_rate/100.0

ev_rates <- edc_rates %>% select(date_concat, eversource_rate)

ev <- res_ev %>% left_join(ev_rates, by = "date_concat")
ev <- ev %>% rename(standard_rate = eversource_rate)

res3 <- dbSendQuery(con, "select supplier, 
	concat(year_charge,month_charge) as date_concat,
	year_charge, 
	month_charge, 
	billed_rate, 
	zipcode,
	num_customers,
	totalkwh,
	edc
from q6.all where edc = 'UI'")
res_ui <- dbFetch(res3)
str(res_ui)

ui_rates <- edc_rates %>% select(date_concat, ui_rate)

ui <- res_ui %>% left_join(ui_rates, by = "date_concat")
ui <- ui %>% rename(standard_rate = ui_rate)

str(ui)

rate_comparison <- rbind(ev,ui)

# clean up
rm(list = c("res_ev","res_ui","ev","ui"))
rm(list = c("res1","res2","res3"))

rate_comparison <- rate_comparison %>% mutate(rate_difference = billed_rate - standard_rate,
                                              overpayment = rate_difference * totalkwh,
                                              year_half = ceiling(as.integer(month_charge)/6))
rate_comparison_simple <- rate_comparison %>% select(-date_concat)
dbWriteTable(con, name = Id(schema = 'final_products', table = 'rate_comparison'), value = rate_comparison_simple, overwrite = TRUE, row.names = FALSE)
filename = paste0(project_dir, "/output/rate_comparison_simple.csv")
write_delim(x = rate_comparison_simple, file = filename, delim = ";")

## overpayment by year ----
overpayment_gross <- rate_comparison %>% filter(overpayment >0) %>%
  group_by(year_charge) %>%
  summarize(gross_overpayment = round(sum(overpayment),0),
            customers_paying_more =sum(num_customers)) %>%
  select(year_charge, gross_overpayment, customers_paying_more)

underpayment_gross <- rate_comparison %>% filter(overpayment <=0) %>%
  group_by(year_charge) %>%
  summarize(gross_underpayment = round(sum(overpayment)),
            customers_paying_less =sum(num_customers)) %>%
  select(year_charge, gross_underpayment, customers_paying_less)

overpayment_net <- rate_comparison %>% 
  group_by(year_charge) %>%
  summarize(net_overpayment = round(sum(overpayment)),
            customers_affected =sum(num_customers),
            total_kwh = sum(totalkwh),
            overpayment_per_kwh = sum(overpayment)*1.0/sum(total_kwh) ) %>%
  select(year_charge, net_overpayment, customers_affected, total_kwh, overpayment_per_kwh)

payment_comparison <- overpayment_gross %>%
  left_join(underpayment_gross, by="year_charge") %>%
  left_join(overpayment_net, by= "year_charge")

filename = paste0(project_dir, "/output/overpayment_by_year.csv")
write_csv(payment_comparison, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_year'), value = payment_comparison, overwrite = TRUE, row.names = FALSE)




## overpayment by supplier ----
overpayment_gross <- rate_comparison %>% filter(overpayment >0) %>%
  group_by(supplier) %>%
  summarize(gross_overpayment = round(sum(overpayment),0),
            customers_paying_more =sum(num_customers)) %>%
  select(supplier, gross_overpayment, customers_paying_more)

underpayment_gross <- rate_comparison %>% filter(overpayment <=0) %>%
  group_by(supplier) %>%
  summarize(gross_underpayment = round(sum(overpayment)),
            customers_paying_less =sum(num_customers)) %>%
  select(supplier, gross_underpayment, customers_paying_less)

overpayment_net <- rate_comparison %>% 
  group_by(supplier) %>%
  summarize(net_overpayment = round(sum(overpayment)),
            customers_affected =sum(num_customers)) %>%
  select(supplier, net_overpayment, customers_affected)

payment_comparison_supplier <- overpayment_gross %>%
  left_join(underpayment_gross, by="supplier") %>%
  left_join(overpayment_net, by= "supplier")

filename = paste0(project_dir, "/output/overpayment_by_supplier.csv")
write_csv(payment_comparison_supplier, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_supplier'), value = payment_comparison_supplier, overwrite = TRUE, row.names = FALSE)




## overpayment by supplier and year ----
overpayment_gross <- rate_comparison %>% filter(overpayment >0) %>%
  group_by(supplier, year_charge) %>%
  summarize(gross_overpayment = round(sum(overpayment),0),
            customers_paying_more =sum(num_customers)) %>%
  select(supplier, year_charge, gross_overpayment, customers_paying_more)

underpayment_gross <- rate_comparison %>% filter(overpayment <=0) %>%
  group_by(supplier, year_charge) %>%
  summarize(gross_underpayment = round(sum(overpayment)),
            customers_paying_less =sum(num_customers)) %>%
  select(supplier, year_charge, gross_underpayment, customers_paying_less)

overpayment_net <- rate_comparison %>% 
  group_by(supplier, year_charge) %>%
  summarize(net_overpayment = round(sum(overpayment)),
            customers_affected =sum(num_customers)) %>%
  select(supplier, year_charge, net_overpayment, customers_affected)

payment_comparison_supplier_year <- overpayment_gross %>%
  left_join(underpayment_gross, by=c("supplier", "year_charge")) %>%
  left_join(overpayment_net, by=c("supplier", "year_charge"))

filename = paste0(project_dir, "/output/overpayment_by_supplier_and year.csv")
write_csv(payment_comparison_supplier_year, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_supplier_and year'), value = payment_comparison_supplier_year, overwrite = TRUE, row.names = FALSE)

# average premium by supplier and year ----
overpayment_net <- rate_comparison %>% 
  group_by(supplier, year_charge) %>%
  summarize(net_overpayment = round(sum(overpayment)),
            bills_rendered =sum(num_customers),
            total_kwh = sum(totalkwh),
            avg_premium = sum(overpayment)/sum(totalkwh)) %>%
  select(supplier, year_charge, net_overpayment, bills_rendered, total_kwh, avg_premium)
filename = paste0(project_dir, "/output/avg_premium_by_supplier_and year.csv")
write_csv(overpayment_net, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'avg_premium_by_supplier_and_year'), value = overpayment_net, overwrite = TRUE, row.names = FALSE)


## overpayment by zipcode ----
overpayment_gross <- rate_comparison %>% filter(overpayment >0) %>%
  group_by(zipcode) %>%
  summarize(gross_overpayment = round(sum(overpayment),0),
            customers_paying_more =sum(num_customers)) %>%
  select(zipcode, gross_overpayment, customers_paying_more)

underpayment_gross <- rate_comparison %>% filter(overpayment <=0) %>%
  group_by(zipcode) %>%
  summarize(gross_underpayment = round(sum(overpayment)),
            customers_paying_less =sum(num_customers)) %>%
  select(zipcode, gross_underpayment, customers_paying_less)

overpayment_net <- rate_comparison %>% 
  group_by(zipcode) %>%
  summarize(net_overpayment = round(sum(overpayment)),
            customers_affected =sum(num_customers)) %>%
  select(zipcode, net_overpayment, customers_affected)

payment_comparison_zipcode <- overpayment_gross %>%
  left_join(underpayment_gross, by="zipcode") %>%
  left_join(overpayment_net, by= "zipcode") %>%
  mutate(zipcode = str_trim(zipcode, side = c("both")))

filename = paste0(project_dir, "/output/overpayment_by_zipcode.csv")
write_csv(payment_comparison_zipcode, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_zipcode'), value = payment_comparison_zipcode, overwrite = TRUE, row.names = FALSE)


## overpayment by zipcode and year  ----
res <- dbSendQuery(con, "select year_charge, zipcode, 
                   num_customers, overpayment from final_products.rate_comparison")
rate_comparison2 <- dbFetch(res)
overpayment_gross <- rate_comparison2 %>% filter(overpayment >0) %>%
  group_by(year_charge, zipcode) %>%
  summarize(gross_overpayment = round(sum(overpayment),0),
            customers_paying_more =sum(num_customers)) %>%
  select(year_charge, zipcode, gross_overpayment, customers_paying_more)

underpayment_gross <- rate_comparison2 %>% filter(overpayment <=0) %>%
  group_by(year_charge, zipcode) %>%
  summarize(gross_underpayment = round(sum(overpayment)),
            customers_paying_less =sum(num_customers)) %>%
  select(year_charge, zipcode, gross_underpayment, customers_paying_less)

overpayment_net <- rate_comparison2 %>% 
  group_by(year_charge, zipcode) %>%
  summarize(net_overpayment = round(sum(overpayment)),
            customers_affected =sum(num_customers)) %>%
  select(year_charge, zipcode, net_overpayment, customers_affected)

payment_comparison_zipcode_year <- overpayment_gross %>%
  left_join(underpayment_gross, by=c("zipcode", "year_charge")) %>%
  left_join(overpayment_net, by= c("zipcode", "year_charge")) %>%
  mutate(zipcode = str_trim(zipcode, side = c("both")))

filename = paste0(project_dir, "/output/overpayment_by_zipcode_and_year.csv")
write_csv(payment_comparison_zipcode_year, filename)



payment_comparison_zipcode_2017 <- payment_comparison_zipcode_year %>%
  filter(year_charge == "2017")
dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_zipcode_2017'), value = payment_comparison_zipcode_2017, overwrite = TRUE, row.names = FALSE)

payment_comparison_zipcode_2018 <- payment_comparison_zipcode_year %>%
  filter(year_charge == "2018")
dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_zipcode_2018'), value = payment_comparison_zipcode_2018, overwrite = TRUE, row.names = FALSE)

payment_comparison_zipcode_2019 <- payment_comparison_zipcode_year %>%
  filter(year_charge == "2019")
dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_zipcode_2019'), value = payment_comparison_zipcode_2019, overwrite = TRUE, row.names = FALSE)

payment_comparison_zipcode_2020 <- payment_comparison_zipcode_year %>%
  filter(year_charge == "2020")
dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_zipcode_2020'), value = payment_comparison_zipcode_2020, overwrite = TRUE, row.names = FALSE)

payment_comparison_zipcode_2021 <- payment_comparison_zipcode_year %>%
  filter(year_charge == "2021")
dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_zipcode_2021'), value = payment_comparison_zipcode_2021, overwrite = TRUE, row.names = FALSE)

# overpayment by 6-month increment ----
 overpayment_gross <- rate_comparison %>% filter(overpayment >0) %>%
   group_by(year_charge, year_half) %>%
   summarize(gross_overpayment = round(sum(overpayment),0),
             customers_paying_more =sum(num_customers)) %>%
   select(year_charge, year_half, gross_overpayment, customers_paying_more)
 
 underpayment_gross <- rate_comparison %>% filter(overpayment <=0) %>%
   group_by(year_charge, year_half) %>%
 summarize(gross_underpayment = round(sum(overpayment)),
             customers_paying_less =sum(num_customers)) %>%
   select(year_charge, year_half, gross_underpayment, customers_paying_less)
 
 overpayment_net <- rate_comparison %>% 
   group_by(year_charge, year_half) %>%
   summarize(net_overpayment = round(sum(overpayment)),
             customers_affected =sum(num_customers)) %>%
   select(year_charge, year_half, net_overpayment, customers_affected)

 payment_comparison <- overpayment_gross %>%
   left_join(underpayment_gross, by=c("year_charge", "year_half")) %>%
   left_join(overpayment_net, by= c("year_charge", "year_half"))
 
 filename = paste0(project_dir, "/output/overpayment_by_6mo_increment.csv")
 write_csv(payment_comparison, filename)
 
 dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_6mo_increment'), value = payment_comparison, overwrite = TRUE, row.names = FALSE)
 
# 2021 distribution of premiums paid ----
 res_prems <- dbSendQuery(con," select 'greater than $0.15 per kWh' as premium,
         sum(num_customers) as bills_rendered
         from final_products.rate_comparison
         where rate_difference > 0.15
         and year_charge = '2021'
         union
         select 'greater than $0.10 per kWh' as premium,
         sum(num_customers) as bills_rendered
         from final_products.rate_comparison
         where rate_difference > 0.10
         and year_charge = '2021'
         union
         select 'greater than $0.05 per kWh' as premium,
         sum(num_customers) as bills_rendered
         from final_products.rate_comparison
         where rate_difference > 0.05
         and year_charge = '2021'
         union
         select 'greater than $0.02 per kWh' as premium,
         sum(num_customers) as bills_rendered
         from final_products.rate_comparison
         where rate_difference > 0.02
         and year_charge = '2021'
         order by premium
         ")
premium_distribution <- dbFetch(res_prems) 
filename = paste0(project_dir, "/output/premiums_distribution_2021.csv")
write_csv(premium_distribution, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'premiums_distribution_2021'), value = premium_distribution, overwrite = TRUE, row.names = FALSE)


print("finished creating rate comparison.")