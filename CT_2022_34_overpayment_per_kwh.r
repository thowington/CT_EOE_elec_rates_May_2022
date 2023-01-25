# CT_2022_34_overpayment_per_kwh.r



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
dbase <- perms$get("this_database")

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = "5432",
  user = user1,
  password = password1,
  dbname = dbase
)
res <- dbSendQuery(con,"select * from acs.crosswalk")
crosswalk <- dbFetch(res)

# overpayment per kwh by ZIP code, 2017 ----
res <- dbSendQuery(con, "select trim(zipcode) zipcode, sum(totalkwh) as totalkwh, sum(overpayment) as net_overpayment
	                        from final_products.rate_comparison
	                        where year_charge = '2017'
	                        group by zipcode")
overpayment_per_kwh_ZIP_2017 <- dbFetch(res)

# overpayment per kwh by ZCTA, 2017 ----
overpayment_per_kwh_zcta_2017 <- overpayment_per_kwh_ZIP_2017 %>%
  left_join(crosswalk, by.x = zipcode, by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(totalkwh = round(sum(totalkwh),0),
            net_overpayment = round(sum(net_overpayment),0),
            overpayment_per_kwh = sum(net_overpayment)/sum(totalkwh)) %>%
  select(zcta, totalkwh, net_overpayment,overpayment_per_kwh)

filename = paste0(project_dir, "/output/overpayment_per_kwh_zcta_2017.csv")
write.csv(overpayment_per_kwh_zcta_2017, filename, row.names = FALSE)
dbWriteTable(con, name = Id(schema = 'final_products', table = 'overpayment_per_kwh_zcta_2017'), value = overpayment_per_kwh_zcta_2017, overwrite = TRUE, row.names = FALSE)



# overpayment per kwh by ZIP code, 2018 ----
res <- dbSendQuery(con, "select trim(zipcode) zipcode, sum(totalkwh) as totalkwh, sum(overpayment) as net_overpayment
	                        from final_products.rate_comparison
	                        where year_charge = '2018'
	                        group by zipcode")
overpayment_per_kwh_ZIP_2018 <- dbFetch(res)

# overpayment per kwh by ZCTA, 2018 ----
overpayment_per_kwh_zcta_2018 <- overpayment_per_kwh_ZIP_2018 %>%
  left_join(crosswalk, by.x = zipcode, by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(totalkwh = round(sum(totalkwh),0),
            net_overpayment = round(sum(net_overpayment),0),
            overpayment_per_kwh = sum(net_overpayment)/sum(totalkwh)) %>%
  select(zcta, totalkwh, net_overpayment,overpayment_per_kwh)

filename = paste0(project_dir, "/output/overpayment_per_kwh_zcta_2018.csv")
write.csv(overpayment_per_kwh_zcta_2018, filename, row.names = FALSE)
dbWriteTable(con, name = Id(schema = 'final_products', table = 'overpayment_per_kwh_zcta_2018'), value = overpayment_per_kwh_zcta_2018, overwrite = TRUE, row.names = FALSE)



# overpayment per kwh by ZIP code, 2019 ----
res <- dbSendQuery(con, "select trim(zipcode) zipcode, sum(totalkwh) as totalkwh, sum(overpayment) as net_overpayment
	                        from final_products.rate_comparison
	                        where year_charge = '2019'
	                        group by zipcode")
overpayment_per_kwh_ZIP_2019 <- dbFetch(res)

# overpayment per kwh by ZCTA, 2019 ----
overpayment_per_kwh_zcta_2019 <- overpayment_per_kwh_ZIP_2019 %>%
  left_join(crosswalk, by.x = zipcode, by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(totalkwh = round(sum(totalkwh),0),
            net_overpayment = round(sum(net_overpayment),0),
            overpayment_per_kwh = sum(net_overpayment)/sum(totalkwh)) %>%
  select(zcta, totalkwh, net_overpayment,overpayment_per_kwh)

filename = paste0(project_dir, "/output/overpayment_per_kwh_zcta_2019.csv")
write.csv(overpayment_per_kwh_zcta_2019, filename, row.names = FALSE)
dbWriteTable(con, name = Id(schema = 'final_products', table = 'overpayment_per_kwh_zcta_2019'), value = overpayment_per_kwh_zcta_2019, overwrite = TRUE, row.names = FALSE)



# overpayment per kwh by ZIP code, 2020 ----
res <- dbSendQuery(con, "select trim(zipcode) zipcode, sum(totalkwh) as totalkwh, sum(overpayment) as net_overpayment
	                        from final_products.rate_comparison
	                        where year_charge = '2020'
	                        group by zipcode")
overpayment_per_kwh_ZIP_2020 <- dbFetch(res)

# overpayment per kwh by ZCTA, 2020 ----
overpayment_per_kwh_zcta_2020 <- overpayment_per_kwh_ZIP_2020 %>%
  left_join(crosswalk, by.x = zipcode, by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(totalkwh = round(sum(totalkwh),0),
            net_overpayment = round(sum(net_overpayment),0),
            overpayment_per_kwh = sum(net_overpayment)/sum(totalkwh)) %>%
  select(zcta, totalkwh, net_overpayment,overpayment_per_kwh)

filename = paste0(project_dir, "/output/overpayment_per_kwh_zcta_2020.csv")
write.csv(overpayment_per_kwh_zcta_2020, filename, row.names = FALSE)
dbWriteTable(con, name = Id(schema = 'final_products', table = 'overpayment_per_kwh_zcta_2020'), value = overpayment_per_kwh_zcta_2020, overwrite = TRUE, row.names = FALSE)



# overpayment per kwh by ZIP code, 2021 ----
res <- dbSendQuery(con, "select trim(zipcode) zipcode, sum(totalkwh) as totalkwh, sum(overpayment) as net_overpayment
	                        from final_products.rate_comparison
	                        where year_charge = '2021'
	                        group by zipcode")
overpayment_per_kwh_ZIP_2021 <- dbFetch(res)

# overpayment per kwh by ZCTA, 2021 ----
overpayment_per_kwh_zcta_2021 <- overpayment_per_kwh_ZIP_2021 %>%
  left_join(crosswalk, by.x = zipcode, by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(totalkwh = round(sum(totalkwh),0),
            net_overpayment = round(sum(net_overpayment),0),
            overpayment_per_kwh = sum(net_overpayment)/sum(totalkwh)) %>%
  select(zcta, totalkwh, net_overpayment,overpayment_per_kwh)

filename = paste0(project_dir, "/output/overpayment_per_kwh_zcta_2021.csv")
write.csv(overpayment_per_kwh_zcta_2021, filename, row.names = FALSE)
dbWriteTable(con, name = Id(schema = 'final_products', table = 'overpayment_per_kwh_zcta_2021'), value = overpayment_per_kwh_zcta_2021, overwrite = TRUE, row.names = FALSE)




# overpayment per kwh by ZCTA, 2021, saving and overpayment separate ----
res_savers <- dbSendQuery(con, "select b.zcta, 
                                sum(num_customers) as bills_with_savings, 
                                sum(totalkwh) as totalkwh_saving, 
                                -sum(overpayment) as total_savings,
                                -sum(overpayment) / sum(num_customers) as savings_per_bill,
                                -sum(overpayment) / sum(totalkwh) as savings_per_kwh
	                        from final_products.rate_comparison a
	                        join acs.crosswalk b
	                        on trim(a.zipcode) = b.zipcode
	                        where year_charge = '2021'
	                        and overpayment <= 0
	                        and num_customers > 0
	                        and totalkwh > 0
	                        group by b.zcta")
overpayment_per_kwh_ZCTA_2021_savers <- dbFetch(res_savers)
overpayment_per_kwh_ZCTA_2021_savers

res_overpayers <- dbSendQuery(con, "select b.zcta, 
                                sum(num_customers) as bills_with_overpay, 
                                sum(totalkwh) as totalkwh_overpay, 
                                sum(overpayment) as total_overpayment,
                                sum(overpayment) / sum(num_customers) as overpayment_per_bill,
                                sum(overpayment) / sum(totalkwh) as overpayment_per_kwh
	                        from final_products.rate_comparison a
	                        join acs.crosswalk b
	                        on trim(a.zipcode) = b.zipcode
	                        where year_charge = '2021'
	                        and overpayment > 0
	                        and num_customers > 0
	                        and totalkwh > 0
	                        group by b.zcta")
overpayment_per_kwh_ZCTA_2021_overpayers <- dbFetch(res_overpayers)
overpayment_per_kwh_ZCTA_2021_overpayers



save_overpay_by_zcta_2021 <- overpayment_per_kwh_ZCTA_2021_savers %>% full_join(overpayment_per_kwh_ZCTA_2021_overpayers,
                                                                                by.x = zcta, by.y = zcta)
save_overpay_by_zcta_2021 %>% filter(zcta == "06001")

filename = paste0(project_dir, "/output/save_overpay_by_zcta_2021.csv")
write.csv(save_overpay_by_zcta_2021, filename, row.names = FALSE)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'save_overpay_by_zcta_2021'), value = save_overpay_by_zcta_2021, overwrite = TRUE, row.names = FALSE)


print("finished script.")
