library(RPostgres)
library(odbc)
library(dplyr)
library(readxl)
library(ConfigParser)
library(stringr)
library(data.table)

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




#### verde
res <- dbSendQuery(con, "drop table if exists q6.verde")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.verde (
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
verde <- read_excel(paste0(project_dir, "/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 5.xlsx"),
                     sheet = "Verde",
                     skip = 0,
                     col_names = TRUE)
str(verde)

#  clean up dates and zip codes
verde$year_charge <- substr(verde$`Month-Year`, 1,4)
verde$month_charge <- substr(verde$`Month-Year`,6,7)
verde$year_commencement <- substr(verde$ContractStartYearMonth, 1,4)
verde$month_commencement <- substr(verde$ContractStartYearMonth, 6,7)

verde$zipcode <- paste0("0",verde$`Zip Code`)
verde <- verde %>% select(-`Zip Code`)

str(verde)



# quick check dates
head(verde %>% select(`Month-Year` , month_charge, year_charge))
tail(verde %>% select(`Month-Year`, month_charge, year_charge))

head(verde %>% select(ContractStartYearMonth, year_commencement, month_commencement))
tail(verde %>% select(ContractStartYearMonth, year_commencement, month_commencement))

# rename columns to match db table
str(verde)
verde <- verde %>% rename(supplier = `Supplier Name`,
                            billed_rate = `Billed Rate`,
                            num_customers = `Customer Count`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term`,
                            servicechargesfees = `Monthly Service Fee`,
                            term_fee = `Termination Amt.`,
                            num_terminations = `Termination Count`)
colnames(verde) <- tolower(colnames(verde))


# select correct columns
verde <- verde %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

# remove non-CT ZIPs
# verde <- verde %>% filter(!zipcode %in% c("08844","07083", "08904",  "07850",  "08071",  "08835",  
#                                          "07924",  "07419",  "019128", "08520",  "08733",  "08079",  
#                                          "07630",  "08005",  "07660",  "07643", "08332",  "07012",
#                                          "019131", "08879",  "07751",  "018929", "017512", "019114",
#                                          "07885",  "07753",  "07834",  "07513",  "017922", "019317",
#                                          "017327"))

#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'verde'), value = verde, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.verde")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading verde.")



