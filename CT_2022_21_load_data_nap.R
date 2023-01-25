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




#### nap
res <- dbSendQuery(con, "drop table if exists q6.nap")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.nap (
              supplier varchar(50),
              year_charge char(4),
              month_charge char(2),
              billed_rate decimal(6,5),
              zipcode char(7),
              num_customers integer,
              totalkwh decimal(10,1),
              year_commencement char(4),
              month_commencement char(2),
              contractterm integer,
              servicechargesfees integer,
              term_fee integer,
              num_terminations integer,
              edc varchar(25))
            ")
dbClearResult(res)

# read raw data
nap <- read_excel(paste0(project_dir, "/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 5.xlsx"),
                     sheet = "NAP",
                     skip = 0,
                     col_names = TRUE)
str(nap)

#  clean up dates and zip codes
nap$year_charge <- substr(nap$MTH_YR, 1,4)
nap$month_charge <- substr(nap$MTH_YR,6,7)

nap$date_exp <- nap$CONTRACT_START_DATE %>% str_replace("January","01") %>%
  str_replace("February","02") %>% str_replace("March","03") %>% str_replace("April","04") %>%
  str_replace("May", "05") %>% str_replace("June", "06") %>% str_replace("July", "07") %>% 
  str_replace("August", "08") %>% str_replace("September", "09") %>%
  str_replace("October", "10") %>% str_replace("November", "11") %>% str_replace("December", "12") 


nap$year_commencement <- paste0("20",substr(nap$date_exp, 4,5))
nap$month_commencement <- substr(nap$date_exp, 1,2)

nap$year_commencement <- nap$year_commencement %>% str_replace("20L",nap$year_charge)
nap$year_commencement <- nap$year_commencement %>% str_replace("20 N",nap$year_charge)
nap$month_commencement <- nap$month_commencement %>% str_replace("NU",nap$month_charge)
nap$month_commencement <- nap$month_commencement %>% str_replace("Se",nap$month_charge)

# nap %>% filter(date_exp %in% c("NULL","See Narrative")) %>% 
#   select(year_charge, month_charge, date_exp, year_commencement, month_commencement)

nap$zipcode <- nap$ZIP_CODE
nap <- nap %>% select(-ZIP_CODE)

str(nap)



# quick check dates
head(nap %>% select(`MTH_YR` , month_charge, year_charge))
tail(nap %>% select(`MTH_YR`, month_charge, year_charge))

head(nap %>% select(`CONTRACT_START_DATE`,year_commencement , month_commencement))
tail(nap %>% select(`CONTRACT_START_DATE`,year_commencement , month_commencement))


# rename columns to match db table
str(nap)
nap2 <- nap %>% rename(supplier = `Supplier`,
                            billed_rate = `RATE`,
                            num_customers = `NBR_OF_CUSTOMERS`,
                            totalkwh = `TOTAL_kWh`,
                            contractterm = `TERM_MTHS`,
                            servicechargesfees = `SRV_CHRG_AND_FEES`,
                            term_fee = `ETF`,
                            num_terminations = `NBR_OF_ETFS`)
colnames(nap2) <- tolower(colnames(nap2))


# select correct columns
nap2 <- nap2 %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

# replace NULLS with 1 to ondicate month-to-month contract
nap2$contractterm <- nap2$contractterm %>% str_replace("NULL","1")
nap2$contractterm <- as.integer(nap2$contractterm)

nap2$term_fee <- nap2$term_fee %>% str_replace("NULL","0")
nap2$term_fee <- as.integer(nap2$term_fee)

nap2$num_terminations <- nap2$num_terminations %>% str_replace("NULL", "0")
nap2$num_terminations <- as.integer(nap2$num_terminations)                                                     

#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'nap'), value = nap2, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.nap")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading nap.")



