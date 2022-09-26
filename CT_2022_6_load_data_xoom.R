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




#### xoom Energy
res <- dbSendQuery(con, "drop table if exists q6.xoom")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.xoom (
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
xoom <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 1.xlsx",
                     sheet = "Xoom",
                     skip = 0,
                     col_names = TRUE)
str(xoom)
unique(xoom$'MONTH-YEAR')

#  clean up dates and zip codes
xoom$date_exp <- xoom$`MONTH-YEAR` %>% str_replace("January","01") %>%
  str_replace("February","02") %>% str_replace("March","03") %>% str_replace("April","04") %>%
  str_replace("May", "05") %>% str_replace("June", "06") %>% str_replace("July", "07") %>% 
  str_replace("August", "08") %>% str_replace("September", "09") %>%
  str_replace("October", "10") %>% str_replace("November", "11") %>% str_replace("December", "12") 
unique(xoom$date_exp)

xoom$comm_date <- xoom$CURRENT_CONTRACT_START_DT %>% str_replace("January","01") %>%
  str_replace("February","02") %>% str_replace("March","03") %>% str_replace("April","04") %>%
  str_replace("May", "05") %>% str_replace("June", "06") %>% str_replace("July", "07") %>% 
  str_replace("August", "08") %>% str_replace("September", "09") %>%
  str_replace("October", "10") %>% str_replace("November", "11") %>% str_replace("December", "12") 
unique(xoom$comm_date)

   

xoom$year_charge <- paste0("20",substr(xoom$date_exp, 4,5))
xoom$month_charge <- substr(xoom$date_exp,1,2)
xoom$year_commencement <- substr(xoom$comm_date, 1,4)
xoom$month_commencement <- substr(xoom$comm_date, 6,7)
xoom$zipcode <- paste0("0", as.character(xoom$`ZIP_CODE`))
xoom <- xoom %>% select(-`ZIP_CODE`)
xoom$ContractTerm <- as.integer(xoom$CONTRACT_TERM)



unique(xoom$ContractTerm)
str(xoom)


# rename columns to match db table
xoom <- xoom %>% rename(supplier = SUPPLIER_NAME,
                            billed_rate = RATE,
                            num_customers = NUMBER_OF_CUSTOMERS ,
                            totalkwh = TOTAL_KWH,
                            servicechargesfees = `SERVICE CHARGES OR OTHER FEES`,
                            term_fee = `TERMINATION FEE`,
                            num_terminations = `# OF TERMINATION FEES`,
                            edc = EDC)
colnames(xoom) <- tolower(colnames(xoom))

# select correct columns
xoom <- xoom %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'xoom'), value = xoom, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
dbGetInfo(con)
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh
	                        from q6.xoom")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading Xoom.")



