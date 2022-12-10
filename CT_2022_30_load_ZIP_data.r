# CT_2022_30_load_ZIP_data.r

# load ACS income data, ZIP_to_ZCTA crosswalk
# join ZCTA to overpayment output
# find total by ZCTA
# join overpayment data to ACS income data
# summarize and display

library(RPostgres)
library(odbc)
library(dplyr)
library(readxl)
library(ConfigParser)

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


#### ACS income data
dbSendQuery(con, "drop schema if exists acs cascade")
dbSendQuery(con, "create schema acs")
dbSendQuery(con, "drop table if exists acs.income")
dbSendQuery(con, "create table acs.income (
              geographic_name varchar(25),
              count_households integer,
              median_hh_income integer,
              zcta character(5))
            ")


# read raw data
ACS_file <- read.table("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/ACS_data/ACSST5Y2020.S1901-Data.csv",
                     skip = 1,
                     sep = ",")
colnames(ACS_file) <- ACS_file[1,]
colnames(ACS_file)
acs_income <- ACS_file %>% select(`Geographic Area Name`, `Estimate!!Households!!Total`, `Estimate!!Households!!Median income (dollars)`) %>%
  mutate(zcta = substr(`Geographic Area Name`, 7, 11)) %>%
  filter(zcta != "phic ") %>%
  rename(geographic_name = `Geographic Area Name`, 
         count_households = `Estimate!!Households!!Total`,
         median_hh_income = `Estimate!!Households!!Median income (dollars)`) %>%
  mutate(count_households = as.integer(count_households),
         median_hh_income = as.integer(median_hh_income))
head(acs_income)
         
dbWriteTable(con, name = Id(schema = 'acs', table = 'income'), value = acs_income, append = TRUE, row.names = FALSE)



#### ZIP_to_ZCTA crosswalk
dbSendQuery(con, "drop table if exists acs.crosswalk")
dbSendQuery(con, "create table acs.crosswalk (
             zipcode char(5),
             po_name varchar(35),
             state char(2),
             zip_type varchar(35),
             zcta char(5),
             zip_join_type varchar(20))
            ")
crosswalk <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/ZIPs/ZiptoZcta_Crosswalk_2021.xlsx",
                           sheet = "ziptozcta2020")
crosswalk <- crosswalk %>% filter(STATE == 'CT') %>%
  rename(zipcode = ZIP_CODE,
         po_name = PO_NAME,
         state = STATE,
         zip_type = ZIP_TYPE,
         zcta = ZCTA)
colnames(crosswalk)

dbWriteTable(con, name = Id(schema = 'acs', table = 'crosswalk'), value = crosswalk, overwrite = TRUE, row.names = FALSE)

# join ZCTA to overpayment output
# find total by ZCTA
overpayment_by_zipcode <- dbReadTable(con, 
                                      name = Id(schema = 'final_products', 
                                                table = 'payment_comparison_by_zipcode'))

overpayment_by_zcta <- overpayment_by_zipcode %>% left_join(crosswalk, 
                                                            by.x = zipcode,
                                                            by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(gross_overpayment = sum(gross_overpayment),
            customers_paying_more = sum(customers_paying_more),
            gross_underpayment = sum(gross_underpayment),
            customers_paying_less = sum(customers_paying_less),
            net_overpayment = sum(net_overpayment),
            customers_affected = sum(customers_affected))

filename = "C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/output/overpayment_by_zcta.csv"
write.csv(overpayment_by_zcta, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_zcta'), value = overpayment_by_zcta, overwrite = TRUE, row.names = FALSE)

##  overpayment by ZCTA and year 2017 ----
overpayment_by_zipcode_2017 <- dbReadTable(con, 
                                      name = Id(schema = 'final_products', 
                                                table = 'payment_comparison_zipcode_2017'))

overpayment_by_zcta_2017 <- overpayment_by_zipcode_2017 %>% left_join(crosswalk, 
                                                            by.x = zipcode,
                                                            by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(gross_overpayment = sum(gross_overpayment),
            customers_paying_more = sum(customers_paying_more),
            gross_underpayment = sum(gross_underpayment),
            customers_paying_less = sum(customers_paying_less),
            net_overpayment = sum(net_overpayment),
            customers_affected = sum(customers_affected))

filename = "C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/output/overpayment_by_zcta_2017.csv"
write.csv(overpayment_by_zcta_2017, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_zcta_2017'), value = overpayment_by_zcta_2017, overwrite = TRUE, row.names = FALSE)


##  overpayment by ZCTA and year 2018 ----
overpayment_by_zipcode_2018 <- dbReadTable(con, 
                                           name = Id(schema = 'final_products', 
                                                     table = 'payment_comparison_zipcode_2018'))

overpayment_by_zcta_2018 <- overpayment_by_zipcode_2018 %>% left_join(crosswalk, 
                                                                      by.x = zipcode,
                                                                      by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(gross_overpayment = sum(gross_overpayment),
            customers_paying_more = sum(customers_paying_more),
            gross_underpayment = sum(gross_underpayment),
            customers_paying_less = sum(customers_paying_less),
            net_overpayment = sum(net_overpayment),
            customers_affected = sum(customers_affected))

filename = paste0(project_dir, "output/overpayment_by_zcta_2018.csv")
write.csv(overpayment_by_zcta_2018, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_zcta_2018'), value = overpayment_by_zcta_2018, overwrite = TRUE, row.names = FALSE)

##  overpayment by ZCTA and year 2019 ----
overpayment_by_zipcode_2019 <- dbReadTable(con, 
                                           name = Id(schema = 'final_products', 
                                                     table = 'payment_comparison_zipcode_2019'))

overpayment_by_zcta_2019 <- overpayment_by_zipcode_2019 %>% left_join(crosswalk, 
                                                                      by.x = zipcode,
                                                                      by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(gross_overpayment = sum(gross_overpayment),
            customers_paying_more = sum(customers_paying_more),
            gross_underpayment = sum(gross_underpayment),
            customers_paying_less = sum(customers_paying_less),
            net_overpayment = sum(net_overpayment),
            customers_affected = sum(customers_affected))

filename = paste0(project_dir, "output/overpayment_by_zcta_2019.csv")
write.csv(overpayment_by_zcta_2019, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_zcta_2019'), value = overpayment_by_zcta_2019, overwrite = TRUE, row.names = FALSE)


##  overpayment by ZCTA and year 2020 ----
overpayment_by_zipcode_2020 <- dbReadTable(con, 
                                           name = Id(schema = 'final_products', 
                                                     table = 'payment_comparison_zipcode_2020'))

overpayment_by_zcta_2020 <- overpayment_by_zipcode_2020 %>% left_join(crosswalk, 
                                                                      by.x = zipcode,
                                                                      by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(gross_overpayment = sum(gross_overpayment),
            customers_paying_more = sum(customers_paying_more),
            gross_underpayment = sum(gross_underpayment),
            customers_paying_less = sum(customers_paying_less),
            net_overpayment = sum(net_overpayment),
            customers_affected = sum(customers_affected))

filename = paste0(project_dir, "output/overpayment_by_zcta_2020.csv")
write.csv(overpayment_by_zcta_2020, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_zcta_2020'), value = overpayment_by_zcta_2020, overwrite = TRUE, row.names = FALSE)


##  overpayment by ZCTA and year 2021 ----
overpayment_by_zipcode_2021 <- dbReadTable(con, 
                                           name = Id(schema = 'final_products', 
                                                     table = 'payment_comparison_zipcode_2021'))

overpayment_by_zcta_2021 <- overpayment_by_zipcode_2021 %>% left_join(crosswalk, 
                                                                      by.x = zipcode,
                                                                      by.y = zipcode) %>%
  group_by(zcta) %>%
  summarize(gross_overpayment = sum(gross_overpayment),
            customers_paying_more = sum(customers_paying_more),
            gross_underpayment = sum(gross_underpayment),
            customers_paying_less = sum(customers_paying_less),
            net_overpayment = sum(net_overpayment),
            customers_affected = sum(customers_affected))

filename = paste0(project_dir, "output/overpayment_by_zcta_2021.csv")
write.csv(overpayment_by_zcta_2021, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_zcta_2021'), value = overpayment_by_zcta_2021, overwrite = TRUE, row.names = FALSE)



# join overpayment data to ACS income data
overpayment_income_by_zcta <- overpayment_by_zcta %>% 
  left_join(acs_income, by.x = zcta, by.y = zcta)

filename = "C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/output/overpayment_income_by_zcta.csv"
write.csv(overpayment_income_by_zcta, filename)

dbWriteTable(con, name = Id(schema = 'final_products', table = 'payment_comparison_by_zcta_income'), value = overpayment_income_by_zcta, overwrite = TRUE, row.names = FALSE)





print("finished loading ZIP data!")
