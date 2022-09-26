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
                                              overpayment = rate_difference * totalkwh)
rate_comparison_simple <- rate_comparison %>% select(-date_concat)
dbWriteTable(con, name = Id(schema = 'final_products', table = 'rate_comparison'), value = rate_comparison_simple, overwrite = TRUE, row.names = FALSE)
filename = "C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/output/rate_comparison_simple.csv"
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
            customers_affected =sum(num_customers)) %>%
  select(year_charge, net_overpayment, customers_affected)

payment_comparison <- overpayment_gross %>%
  left_join(underpayment_gross, by="year_charge") %>%
  left_join(overpayment_net, by= "year_charge")

filename = "C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/output/overpayment_by_year.csv"
write_csv(payment_comparison, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_year'), value = payment_comparison, overwrite = TRUE, row.names = FALSE)


## overpayment by supplier
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

filename = "C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/output/overpayment_by_supplier.csv"
write_csv(payment_comparison_supplier, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_supplier'), value = payment_comparison_supplier, overwrite = TRUE, row.names = FALSE)


## overpayment by zipcode
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

filename = "C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/output/overpayment_by_zipcode.csv"
write_csv(payment_comparison_zipcode, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_zipcode'), value = payment_comparison_zipcode, overwrite = TRUE, row.names = FALSE)

print("finished creating rate comparison.")