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

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = "5432",
  user = user1,
  password = password1,
  dbname = "ct_2022"
)

# overpayment per kwh by ZIP code, 2021
res <- dbSendQuery(con, "select trim(zipcode) zipcode, sum(totalkwh) as totalkwh, sum(overpayment) as net_overpayment
	                        from final_products.rate_comparison
	                        where year_charge = '2021'
	                        group by zipcode")
Overpayment_per_kwh_ZIP_2021 <- dbFetch(res)

# overpayment per kwh by ZCTA, 2021
Overpayment_per_kwh_ZCTA_2021 <- Overpayment_per_kwh_ZIP_2021 %>%
  left_join(crosswalk, by.x = zipcode, by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(totalkwh = round(sum(totalkwh),0),
            net_overpayment = round(sum(net_overpayment),0),
            overpayment_per_kwh = sum(net_overpayment)/sum(totalkwh)) %>%
  select(zcta, totalkwh, net_overpayment,overpayment_per_kwh)

filename = paste0(project_dir, "output/overpayment_per_kwh_ZCTA_2021.csv")
write.csv(Overpayment_per_kwh_ZCTA_2021, filename, row.names = FALSE)

