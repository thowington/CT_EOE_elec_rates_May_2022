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




#### viridian
res <- dbSendQuery(con, "drop table if exists q6.viridian")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.viridian (
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
viridian <- read_excel(paste0(project_dir, "/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 2.xlsx"),
                     sheet = "Viridian",
                     skip = 0,
                     col_names = TRUE)
str(viridian)

#  clean up dates and zip codes
viridian$year_charge <- substr(viridian$`Month-Year`, 1,4)
viridian$month_charge <- substr(viridian$`Month-Year`,6,7)
viridian$year_commencement <- substr(viridian$`Current Contract Commencement Date (month year)`, 1,4)
viridian$month_commencement <- substr(viridian$`Current Contract Commencement Date (month year)`, 6,7)
viridian$zipcode <- paste0("0", as.character(viridian$`Zip Code`))
viridian <- viridian %>% select(-`Zip Code`)

str(viridian)


# quick check dates
head(viridian %>% select(`Month-Year` , month_charge, year_charge))
tail(viridian %>% select(`Month-Year`, month_charge, year_charge))

head(viridian %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))
tail(viridian %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))


# rename columns to match db table

viridian <- viridian %>% rename(supplier = `Supplier name`,
                            billed_rate = Rate,
                            num_customers = `Number of Customers`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term in Months (i.e., length of contract)`,
                            servicechargesfees = `Service Charge or Other Fees (excluding Termination Fees)`,
                            term_fee = `Termination Fees`,
                            num_terminations = `# of Terminations`)
colnames(viridian) <- tolower(colnames(viridian))

# select correct columns
viridian <- viridian %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

# remove records for zip code 46513
#viridian <- viridian %>% filter(!zipcode %in% c('046513', '060795', '07410'))

#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'viridian'), value = viridian, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.viridian")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading viridian.")



