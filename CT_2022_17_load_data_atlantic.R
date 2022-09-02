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




#### atlantic
res <- dbSendQuery(con, "drop table if exists q6.atlantic")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.atlantic (
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
atlantic <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 4.xlsx",
                     sheet = "Atlantic",
                     skip = 0,
                     col_names = TRUE)
str(atlantic)
unique(atlantic$`Month-Year`)

#  clean up dates and zip codes

atlantic$year_charge <- substr(atlantic$`Month-Year`, 1,4)
atlantic$month_charge <- substr(atlantic$`Month-Year`,6,7)
atlantic$year_commencement <- substr(atlantic$`Current Contract Commencement Date (month-year)`, 1,4)
atlantic$month_commencement <- substr(atlantic$`Current Contract Commencement Date (month-year)`, 6,7)

atlantic$zipcode <- paste0("0",atlantic$`Zip Code`)
atlantic <- atlantic %>% select(-`Zip Code`)

str(atlantic)
#unique(atlantic$zipcode)


# quick check dates
head(atlantic %>% select(`Month-Year` , month_charge, year_charge))
tail(atlantic %>% select(`Month-Year`, month_charge, year_charge))

head(atlantic %>% select(`Current Contract Commencement Date (month-year)`,year_commencement , month_commencement))
tail(atlantic %>% select(`Current Contract Commencement Date (month-year)`,year_commencement , month_commencement))


# rename columns to match db table
str(atlantic)
atlantic <- atlantic %>% rename(supplier = `Supplier Name`,
                            billed_rate = Rate,
                            num_customers = `Number of Customers`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term in Months (i.e., length of contract)`,
                            servicechargesfees = `Service Charges or Other Fees (excluding Termination Fees)`,
                            term_fee = `Termination Fee`,
                            num_terminations = `Number of termination fees charged associated with those customer contracts during that billing cycle`)
colnames(atlantic) <- tolower(colnames(atlantic))


# select correct columns
atlantic <- atlantic %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)


#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'atlantic'), value = atlantic, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.atlantic")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished.")



