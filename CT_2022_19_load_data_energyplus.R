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




#### energyplus
res <- dbSendQuery(con, "drop table if exists q6.energyplus")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.energyplus (
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
energyplus <- read_excel(paste0(project_dir, "/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 4.xlsx"),
                     sheet = "Energy Plus",
                     skip = 0,
                     col_names = TRUE)
str(energyplus)


# #  clean up dates and zip codes


energyplus$year_charge <- substr(energyplus$`MonthYear`, 1,4)
energyplus$month_charge <- substr(energyplus$`MonthYear`,6,7)

energyplus$CurrentContractStart <- energyplus$CurrentContractStart %>% 
  str_replace("Month-To-Month",paste(energyplus$year_charge,energyplus$month_charge, sep = "-"))
unique(energyplus$CurrentContractStart)

energyplus$year_commencement <- substr(energyplus$CurrentContractStart, 1,4)
energyplus$month_commencement <- substr(energyplus$CurrentContractStart, 6,7)

energyplus$CurrentContractNumberOfMonths <- energyplus$CurrentContractNumberOfMonths %>% str_replace("Month-To-Month","1")
energyplus$contractterm <- as.integer(energyplus$CurrentContractNumberOfMonths)

str(energyplus)


# quick check dates
head(energyplus %>% select(`MonthYear` , month_charge, year_charge))
tail(energyplus %>% select(`MonthYear`, month_charge, year_charge))

head(energyplus %>% select(CurrentContractStart,year_commencement , month_commencement))
tail(energyplus %>% select(CurrentContractStart,year_commencement , month_commencement))


# rename columns to match db table
str(energyplus)
energyplus <- energyplus %>% rename(supplier = `BrandName`,
                            billed_rate = Rate,
                            num_customers = `NumberOfCustomers`,
                            totalkwh = `TotalkWh`,
                            servicechargesfees = SvcCharges,
                            term_fee = EarlyTerminationFee,
                            num_terminations = NumberOfTerminations)
colnames(energyplus) <- tolower(colnames(energyplus))


# select correct columns
energyplus <- energyplus %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)



#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'energyplus'), value = energyplus, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.energyplus")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading energyplus.")



