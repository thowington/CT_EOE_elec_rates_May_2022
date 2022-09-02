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




#### Residents
res <- dbSendQuery(con, "drop table if exists q6.residents")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.residents (
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
residents <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 3.xlsx",
                     sheet = "Residents",
                     skip = 0,
                     col_names = TRUE)
str(residents)

#  clean up dates and zip codes
residents$date_exp <- residents$`Month-Year` %>% str_replace("January","01") %>%
  str_replace("February","02") %>% str_replace("March","03") %>% str_replace("April","04") %>%
  str_replace("May", "05") %>% str_replace("June", "06") %>% str_replace("July", "07") %>% 
  str_replace("August", "08") %>% str_replace("September", "09") %>%
  str_replace("October", "10") %>% str_replace("November", "11") %>% str_replace("December", "12") 


residents$year_charge <- paste0("20",substr(residents$date_exp, 4,5))
residents$month_charge <- substr(residents$date_exp,1,2)
residents$year_commencement <- "NA"
residents$month_commencement <- "NA"
residents$zipcode <- paste0("0", as.character(residents$`Zip Code`))
residents <- residents %>% select(-`Zip Code`)

str(residents)


# quick check dates
head(residents %>% select(`Month-Year` , month_charge, year_charge))
tail(residents %>% select(`Month-Year`, month_charge, year_charge))

head(residents %>% select(`Current Contract Commencement Date`,year_commencement , month_commencement))
tail(residents %>% select(`Current Contract Commencement Date`,year_commencement , month_commencement))


# rename columns to match db table
str(residents)
residents <- residents %>% rename(supplier = `Supplier Name`,
                            billed_rate = Rate,
                            num_customers = `Number of\r\nCustomers`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term\r\nin Months`,
                            servicechargesfees = `Service Charges\r\nor Fees`,
                            term_fee = `Termination Fee`,
                            num_terminations = `# of Terminations`)
colnames(residents) <- tolower(colnames(residents))


# select correct columns
residents <- residents %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)


#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'residents'), value = residents, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.residents")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished.")



