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




#### public
res <- dbSendQuery(con, "drop table if exists q6.public")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.public (
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
public <- read_excel(paste0(project_dir, "/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 2.xlsx"),
                     sheet = "PP",
                     skip = 0,
                     col_names = TRUE)
str(public)

#  clean up dates and zip codes
public$year_charge <- substr(public$`Month-Year`, 1,4)
public$month_charge <- substr(public$`Month-Year`,6,7)

# unique(public$`Current Contract Commencement Date (month year)`)

public$`Current Contract Commencement Date (month year)` <- public$`Current Contract Commencement Date (month year)` %>% 
  str_replace("See Response to EOE-6","")
public$contract_commencement <- as.Date(as.numeric(public$`Current Contract Commencement Date (month year)`),origin = "1900-01-01")
unique(public$contract_commencement)

public$year_commencement <- substr(public$contract_commencement, 1, 4)
public$month_commencement <- substr(public$contract_commencement, 6, 7)
public$zipcode <- paste0("0", as.character(public$`Zip Code`))
public <- public %>% select(-`Zip Code`)

str(public)


# quick check dates
head(public %>% select(`Month-Year` , month_charge, year_charge))
tail(public %>% select(`Month-Year`, month_charge, year_charge))

head(public %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))
tail(public %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))


# rename columns to match db table

public <- public %>% rename(supplier = `Supplier name`,
                            billed_rate = Rate,
                            num_customers = `Number of Customers`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term in Months (i.e., length of contract)`,
                            servicechargesfees = `Service Charge or Other Fees (excluding Termination Fees)`,
                            term_fee = `Termination Fees`,
                            num_terminations = `# of Terminations`)
colnames(public) <- tolower(colnames(public))

#  remove unknowns
public$contractterm <- substr(public$contractterm,  "Unknown", "NA")
public$contractterm <- as.integer(public$contractterm)

# select correct columns
public <- public %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

# change the supplier name
public$supplier <- 'Public Power'

#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'public'), value = public, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.public")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading public.")



