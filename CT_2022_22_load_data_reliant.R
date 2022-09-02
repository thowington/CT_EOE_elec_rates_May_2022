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

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = "5432",
  user = user1,
  password = password1,
  dbname = "ct_2022"
)




#### reliant
res <- dbSendQuery(con, "drop table if exists q6.reliant")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.reliant (
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
              edc varchar(30))
            ")
dbClearResult(res)

# read raw data
reliant <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 5.xlsx",
                     sheet = "Reliant",
                     skip = 0,
                     col_names = TRUE)
str(reliant)

#  clean up dates and zip codes
reliant$year_charge <- substr(reliant$MonthYear, 1,4)
reliant$month_charge <- substr(reliant$MonthYear,6,7)

reliant %>% filter(CurrentContractStart == "Month-To-Month") %>% mutate(CurrentContractStart = MonthYear) 
reliant$year_commencement <- substr(reliant$CurrentContractStart, 1,4)
reliant$month_commencement <- substr(reliant$CurrentContractStart, 6,7)

reliant$zipcode <- paste0("0",reliant$`ZipCode`)
reliant <- reliant %>% select(-`ZipCode`)

str(reliant)



# quick check dates
head(reliant %>% select(`MonthYear` , month_charge, year_charge))
tail(reliant %>% select(`MonthYear`, month_charge, year_charge))

head(reliant %>% select(CurrentContractStart, year_commencement, month_commencement))
tail(reliant %>% select(CurrentContractStart, year_commencement, month_commencement))

# rename columns to match db table
str(reliant)
reliant <- reliant %>% rename(supplier = `BrandName`,
                            billed_rate = Rate,
                            num_customers = `NumberOfCustomers`,
                            totalkwh = `TotalkWh`,
                            contractterm = CurrentContractNumberOfMonths,
                            servicechargesfees = SvcCharges,
                            term_fee = EarlyTerminationFee,
                            num_terminations = NumberOfTerminations)
colnames(reliant) <- tolower(colnames(reliant))


# select correct columns
reliant <- reliant %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)
unique(reliant$contractterm)
reliant$contractterm <- reliant$contractterm %>% str_replace("Month-To-Month","1")
reliant$contractterm <- as.integer(reliant$contractterm)


#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'reliant'), value = reliant, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.reliant")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished.")



