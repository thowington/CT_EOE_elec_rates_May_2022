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




#### clearview
res <- dbSendQuery(con, "drop table if exists q6.clearview")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.clearview (
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
clearview <- read_excel(paste0(project_dir, "/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 5.xlsx"),
                     sheet = "Clearview",
                     skip = 0,
                     col_names = TRUE)
str(clearview)

#  clean up dates and zip codes
clearview$date_exp <- clearview$`Month-Year` %>% str_replace("January","01") %>%
  str_replace("February","02") %>% str_replace("March","03") %>% str_replace("April","04") %>%
  str_replace("May", "05") %>% str_replace("June", "06") %>% str_replace("July", "07") %>% 
  str_replace("August", "08") %>% str_replace("September", "09") %>%
  str_replace("October", "10") %>% str_replace("November", "11") %>% str_replace("December", "12") 

clearview$year_charge <- substr(clearview$date_exp, 4,7)
clearview$month_charge <- substr(clearview$date_exp,1,2)

clearview$date_comm  <- clearview$`Current Contract Commencement Date (month year)` %>% str_replace("January","01") %>%
  str_replace("February","02") %>% str_replace("March","03") %>% str_replace("April","04") %>%
  str_replace("May", "05") %>% str_replace("June", "06") %>% str_replace("July", "07") %>% 
  str_replace("August", "08") %>% str_replace("September", "09") %>%
  str_replace("October", "10") %>% str_replace("November", "11") %>% str_replace("December", "12")

clearview$year_commencement <- substr(clearview$date_comm, 4,7)
clearview$month_commencement <- substr(clearview$date_comm, 1,2)

clearview$year_commencement <- clearview$year_commencement %>% na_if("L")
clearview$month_commencement <- clearview$month_commencement %>% na_if("NU")

clearview$zipcode <- paste0("0",clearview$`ZipCode`)
clearview <- clearview %>% select(-`ZipCode`)


# quick check dates
head(clearview %>% select(`Month-Year` , month_charge, year_charge))
tail(clearview %>% select(`Month-Year`, month_charge, year_charge))

head(clearview %>% select(`Current Contract Commencement Date (month year)`, year_commencement, month_commencement))
tail(clearview %>% select(`Current Contract Commencement Date (month year)`, year_commencement, month_commencement))

# rename columns to match db table
str(clearview)
clearview <- clearview %>% rename(supplier = `Supplier Name`,
                            billed_rate = `Rate`,
                            num_customers = `Customers subscribed to Green Services`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term in Months (i.e. length of contract)`,
                            servicechargesfees = `Service Charges or Other Fees (excluding Termination Fess)`,
                            term_fee = `Termination Fee`,
                            num_terminations = `# of Terminations`)
colnames(clearview) <- tolower(colnames(clearview))

clearview$contractterm <- clearview$contractterm %>% str_replace("NULL", "1")
clearview$contractterm <- as.integer(clearview$contractterm)


# select correct columns
clearview <- clearview %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

# remove non-CT ZIPs
# clearview <- clearview %>% filter(!zipcode %in% c("076605","08062", "07898", "060511", "05515",
#                               "07447", "05902", "034113", "0NA", "010012", "010671", "01606", 
#                               "034288", "0605", "066708", "010065" ))

clearview$term_fee <- as.integer(clearview$term_fee)

#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'clearview'), value = clearview, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.clearview")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading clearview.")



