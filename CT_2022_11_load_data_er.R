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




#### Energy Rewards
res <- dbSendQuery(con, "drop table if exists q6.er")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.er (
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
er <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 2.xlsx",
                     sheet = "EE",
                     skip = 0,
                     col_names = TRUE)
str(er)

#  clean up dates and zip codes
er$year_charge <- substr(er$`Month-Year`, 1,4)
er$month_charge <- substr(er$`Month-Year`,6,7)
er$year_commencement <- substr(er$`Current Contract Commencement Date (month year)`, 1, 4)
er$month_commencement <- substr(er$`Current Contract Commencement Date (month year)`, 6, 7)
er$zipcode <- paste0("0", as.character(er$`Zip Code`))
er <- er %>% select(-`Zip Code`)

str(er)


# quick check dates
head(er %>% select(`Month-Year` , month_charge, year_charge))
tail(er %>% select(`Month-Year`, month_charge, year_charge))

head(er %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))
tail(er %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))


# rename columns to match db table
str(er)
er <- er %>% rename(supplier = `Supplier name`,
                            billed_rate = Rate,
                            num_customers = `Number of Customers`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term in Months (i.e., length of contract)`,
                            servicechargesfees = `Service Charge or Other Fees (excluding Termination Fees)`,
                            term_fee = `Termination Fees`,
                            num_terminations = `# of Terminations`)
colnames(er) <- tolower(colnames(er))


# select correct columns
er <- er %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)


#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'er'), value = er, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.er")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished.")



