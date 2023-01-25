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




#### Think Energy
res <- dbSendQuery(con, "drop table if exists q6.think")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.think (
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
think <- read_excel(paste0(project_dir, "/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 1.xlsx"),
                     sheet = "Think Energy",
                     skip = 0,
                     col_names = TRUE)
str(think)
unique(think$'Month-Year')

#  clean up dates and zip codes
think$date_exp <- think$`Month-Year` %>% str_replace("January","01") %>%
  str_replace("February","02") %>% str_replace("March","03") %>% str_replace("April","04") %>%
  str_replace("May", "05") %>% str_replace("June", "06") %>% str_replace("July", "07") %>% 
  str_replace("August", "08") %>% str_replace("September", "09") %>%
  str_replace("October", "10") %>% str_replace("November", "11") %>% str_replace("December", "12") 
unique(think$date_exp)

think$comm_date <- think$CurrentContractCommencementDate %>% str_replace("January","01") %>%
  str_replace("February","02") %>% str_replace("March","03") %>% str_replace("April","04") %>%
  str_replace("May", "05") %>% str_replace("June", "06") %>% str_replace("July", "07") %>% 
  str_replace("August", "08") %>% str_replace("September", "09") %>%
  str_replace("October", "10") %>% str_replace("November", "11") %>% str_replace("December", "12") 
unique(think$comm_date)

   

think$year_charge <- paste0("20",substr(think$date_exp, 4,5))
think$month_charge <- substr(think$date_exp,1,2)
think$year_commencement <- paste0('20',substr(think$comm_date, 4,5))
think$month_commencement <- substr(think$comm_date, 1,2)
think$zipcode <- paste0("0", as.character(think$`ZipCode`))
think <- think %>% select(-`ZipCode`)
think$ContractTerm <- think$ContractTerm %>% str_replace(" Months","") %>% str_replace(" Month","")
think$ContractTerm <- as.integer(think$ContractTerm)


unique(think$ContractTerm)
str(think)


# rename columns to match db table
think <- think %>% rename(supplier = SupplierName,
                            billed_rate = Rate,
                            num_customers = NumberOfCustomers,
                            totalkwh = TotalkWh,
                            contractterm = ContractTerm,
                            servicechargesfees = ServiceCharges,
                            term_fee = TerminationFee,
                            num_terminations = `# of Termination Fees Charged`,
                            edc = EDC)
colnames(think) <- tolower(colnames(think))

# select correct columns
think <- think %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'think'), value = think, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
dbGetInfo(con)
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh
	                        from q6.think")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading think.")



