library(RPostgres)
library(odbc)
library(dplyr)
library(readxl)
library(ConfigParser)

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




#### wattifi
res <- dbSendQuery(con, "drop table if exists q6.wattifi")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.wattifi (
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
wattifi <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 1.xlsx",
                     sheet = "Wattifi",
                     skip = 0,
                     col_names = TRUE)
str(wattifi)

#  clean up dates and zip codes
wattifi$year_charge <- substr(wattifi$`Month-Year`, 1,4)
wattifi$month_charge <- substr(wattifi$`Month-Year`,6,7)
wattifi$year_commencement <- substr(wattifi$`Current Contract Commencement Date (month-year)`, 1,4)
wattifi$month_commencement <- substr(wattifi$`Current Contract Commencement Date (month-year)`, 6,7)
wattifi$zipcode <- paste0("0", as.character(wattifi$`Zip Codes`))
wattifi <- wattifi %>% select(-`Zip Codes`)

str(wattifi)


# quick check dates
head(wattifi %>% select(`Month-Year` , month_charge, year_charge))
tail(wattifi %>% select(`Month-Year`, month_charge, year_charge))

head(wattifi %>% select(`Current Contract Commencement Date (month-year)`,year_commencement , month_commencement))
tail(wattifi %>% select(`Current Contract Commencement Date (month-year)`,year_commencement , month_commencement))


# rename columns to match db table

wattifi <- wattifi %>% rename(supplier = `Supplier Name`,
                            billed_rate = Rate,
                            num_customers = `Number of Customers`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term in Months (i.e. length of contract)`,
                            servicechargesfees = `Service Charges or Other Fees`,
                            term_fee = `Termination Fee`,
                            num_terminations = `# of Terminations`)
colnames(wattifi) <- tolower(colnames(wattifi))

# select correct columns
wattifi <- wattifi %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)


#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'wattifi'), value = wattifi, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.wattifi")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished.")



