library(RPostgres)
library(odbc)
library(dplyr)
library(readxl)
library(ConfigParser)
library(stringr)

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




#### edc rates
dbSendQuery(con, "drop table if exists edc_data.edc_rates")
dbSendQuery(con, "create table edc_data.edc_rates (
              year char(4),
              month char(2),
              eversource_rate decimal(6,4),
              ui_rate decimal(6,4),
              date_concat text)
            ")

# read raw data
edc_rates <- read_excel(paste0(project_dir, "/from_EOE/2 Supplier and EDC Rates Combined - vs2.xlsx"),
                     sheet = "EDC Rates",
                     skip = 0,
                     col_names = TRUE)
str(edc_rates)

edc_rates$month <- str_pad(edc_rates$month, width=2, side="left", pad="0")
edc_rates$date_concat <- paste0(edc_rates$year, edc_rates$month)


dbWriteTable(con, name = Id(schema = 'edc_data', table = 'edc_rates'), value = edc_rates, overwrite = TRUE, row.names = FALSE)


#check totals
res <- dbSendQuery(con, "select * from edc_data.edc_rates")
result <- dbFetch(res)
result

dbDisconnect(con)

print('finished loading EDC rates.')



