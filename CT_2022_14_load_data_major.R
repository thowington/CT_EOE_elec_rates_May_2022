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




#### major
res <- dbSendQuery(con, "drop table if exists q6.major")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.major (
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
major <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 3.xlsx",
                     sheet = "Major",
                     skip = 0,
                     col_names = TRUE)
str(major)

#  clean up dates and zip codes
major$year_charge <- substr(major$`Month-Year`, 1,4)
major$month_charge <- substr(major$`Month-Year`,6,7)
major$year_commencement <- substr(major$`Contract Start Year Month`, 1,4)
major$month_commencement <- substr(major$`Contract Start Year Month`, 6,7)
major$zipcode <- paste0("0", major$`Zip Code`)
major <- major %>% select(-`Zip Code`)

#major <- major %>% filter(!zipcode == "029210")

str(major)



# quick check dates
head(major %>% select(`Month-Year` , month_charge, year_charge))
tail(major %>% select(`Month-Year`, month_charge, year_charge))

head(major %>% select(`Contract Start Year Month`,year_commencement , month_commencement))
tail(major %>% select(`Contract Start Year Month`,year_commencement , month_commencement))


# rename columns to match db table
str(major)
major <- major %>% rename(supplier = `Supplier Name`,
                            billed_rate = Rate,
                            num_customers = `Customer Count`,
                            totalkwh = `Total kWh`,
                            contractterm = `ContractTerm`,
                            servicechargesfees = `Service Charge`,
                            term_fee = MsfAmt,
                            num_terminations = ETFAmt)
colnames(major) <- tolower(colnames(major))


# select correct columns
major <- major %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)


#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'major'), value = major, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.major")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading major.")



