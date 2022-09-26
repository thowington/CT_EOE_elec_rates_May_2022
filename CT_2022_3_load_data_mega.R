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




#### Mega
res <- dbSendQuery(con, "drop table if exists q6.mega")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.mega (
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
mega <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 1.xlsx",
                     sheet = "Mega",
                     skip = 0,
                     col_names = TRUE)
str(mega)

#  clean up dates and zip codes
mega$year_charge <- substr(mega$`Month-Year`, 1,4)
mega$month_charge <- substr(mega$`Month-Year`,6,7)
mega$year_commencement <- substr(mega$`Current Contract Commencement Date (Month Year)`, 1,4)
mega$month_commencement <- substr(mega$`Current Contract Commencement Date (Month Year)`, 6,7)
mega$zipcode <- paste0("0", as.character(mega$`Zip Code`))
mega <- mega %>% select(-`Zip Code`)

str(mega)


# quick check dates
head(mega %>% select(`Month-Year` , month_charge, year_charge))
tail(mega %>% select(`Month-Year`, month_charge, year_charge))

head(mega %>% select(`Current Contract Commencement Date (Month Year)`,year_commencement , month_commencement))
tail(mega %>% select(`Current Contract Commencement Date (Month Year)`,year_commencement , month_commencement))


# rename columns to match db table

mega <- mega %>% rename(supplier = `Supplier Name`,
                            billed_rate = Rate,
                            num_customers = `Number of Customers`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term (i.e. length of contract`,
                            servicechargesfees = `Service Charges or Other Fees (excluding termination fees)`,
                            term_fee = `Termination fee`,
                            num_terminations = `# of Terminations`)
colnames(mega) <- tolower(colnames(mega))

# select correct columns
mega <- mega %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

# remove records for zip code 46513
# mega <- mega %>% filter(!zipcode %in% c('046513', '060795', '07410'))

#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'mega'), value = mega, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.mega")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading mega.")



