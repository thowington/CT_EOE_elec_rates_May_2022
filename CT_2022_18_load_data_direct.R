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




#### direct
res <- dbSendQuery(con, "drop table if exists q6.direct")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.direct (
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
direct <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 4.xlsx",
                     sheet = "Direct Energy",
                     skip = 0,
                     col_names = TRUE)
str(direct)
unique(direct$`Month-Year`)

# #  clean up dates and zip codes


direct$year_charge <- substr(direct$`Month-Year`, 1,4)
direct$month_charge <- substr(direct$`Month-Year`,6,7)

direct$date_exp <- direct$`Current Contract Commencement Date (month year)` %>% str_replace("January","01") %>%
  str_replace("February","02") %>% str_replace("March","03") %>% str_replace("April","04") %>%
  str_replace("May", "05") %>% str_replace("June", "06") %>% str_replace("July", "07") %>%
  str_replace("August", "08") %>% str_replace("September", "09") %>%
  str_replace("October", "10") %>% str_replace("November", "11") %>% str_replace("December", "12")

direct$year_commencement <- paste0("20",substr(direct$date_exp, 4,5))
direct$month_commencement <- substr(direct$date_exp, 1,2)

direct$zipcode <- paste0("0",direct$`Zip Code`)
direct <- direct %>% select(-`Zip Code`)

str(direct)
#unique(direct$zipcode)


# quick check dates
head(direct %>% select(`Month-Year` , month_charge, year_charge))
tail(direct %>% select(`Month-Year`, month_charge, year_charge))

head(direct %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))
tail(direct %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))


# rename columns to match db table
str(direct)
direct <- direct %>% rename(supplier = `Supplier`,
                            billed_rate = Rate,
                            num_customers = `Number of Customers`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term in Months`,
                            servicechargesfees = `Service Charges or Other Fees (excluding Termination Fees)`,
                            term_fee = `Termination Fees`,
                            num_terminations = `# of Terminations`)
colnames(direct) <- tolower(colnames(direct))


# select correct columns
direct <- direct %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)


# replace some NULL values
direct$contractterm <- direct$contractterm %>% str_replace("NULL", "99")
direct$contractterm <- as.integer(direct$contractterm)



#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'direct'), value = direct, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.direct")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading direct.")



