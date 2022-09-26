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

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = "5432",
  user = user1,
  password = password1,
  dbname = "ct_2022"
)




#### consolidated answers to EOE-9
dbSendQuery(con, "drop schema if exists q9 cascade")
dbSendQuery(con, "create schema q9")
dbSendQuery(con, "drop table if exists q9.all")
dbSendQuery(con, "create table q9.all (
              supplier varchar(50),
              year_charge char(4),
              num_customers integer,
              specific_commitment varchar(25),
              billed_rate decimal(6,5),
              totalkwh decimal(10,1))
            ")

# read raw data
q9_all <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/supplier_responses/EOE_9_consolidation.xlsx",
                     sheet = "Sheet3",
                     skip = 0,
                     col_names = TRUE)
str(q9_all)




dbWriteTable(con, name = Id(schema = 'q9', table = 'all'), value = q9_all, append = TRUE, row.names = FALSE)

# cleanup
dbSendQuery(con, "delete from q9.all
                    where totalkwh < 0
                    or billed_rate < 0")

#check totals
res <- dbSendQuery(con, "select year_charge,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh
	                        from q9.all
                          group by year_charge
                          order by year_charge")
result <- dbFetch(res)
result
# dbClearResult(con)
# dbDisconnect(con)

print("finished loading EOE-9 data.")

