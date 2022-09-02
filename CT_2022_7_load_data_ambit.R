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




#### ambit Energy
res <- dbSendQuery(con, "drop table if exists q6.ambit")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.ambit (
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
ambit <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 2.xlsx",
                     sheet = "Ambit",
                     skip = 0,
                     col_names = TRUE)
str(ambit)
unique(ambit$'Month-Year')

#  clean up dates and zip codes
ambit$year_charge <- substr(ambit$`Month-Year`, 1,4)
ambit$month_charge <- substr(ambit$`Month-Year`,6,7)
ambit$year_commencement <- substr(ambit$`Current Contract Commencement Date (month year)`, 1,4)
ambit$month_commencement <- substr(ambit$`Current Contract Commencement Date (month year)`, 6,7)
ambit$zipcode <- paste0("0", as.character(ambit$`Zip Code`))
ambit <- ambit %>% select(-`Zip Code`)
ambit$ContractTerm <- as.integer(ambit$`Contract Term in Months (i.e., length of contract)`)



unique(ambit$ContractTerm)
str(ambit)


# rename columns to match db table
ambit <- ambit %>% rename(supplier = `Supplier Name`,
                            billed_rate = Rate,
                            num_customers = `Number of Customers` ,
                            totalkwh = `Total kWh`,
                            servicechargesfees = `Service Charges or Other Fees (excluding Termination Fees)`,
                            term_fee = `Termination Fee`,
                            num_terminations = `# of Terminations`,
                            edc = EDC)
colnames(ambit) <- tolower(colnames(ambit))

# select correct columns
ambit <- ambit %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'ambit'), value = ambit, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
dbGetInfo(con)
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh
	                        from q6.ambit")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished.")



