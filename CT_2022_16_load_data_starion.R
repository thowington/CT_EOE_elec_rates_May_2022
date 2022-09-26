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




#### starion
res <- dbSendQuery(con, "drop table if exists q6.starion")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.starion (
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
starion <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 3.xlsx",
                     sheet = "Starion",
                     skip = 0,
                     col_names = TRUE)
str(starion)

#  clean up dates and zip codes
starion$date_exp <- starion$`Month-Year` %>% str_replace("Jan","01") %>%
  str_replace("Feb","02") %>% str_replace("Mar","03") %>% str_replace("Apr","04") %>%
  str_replace("May", "05") %>% str_replace("Jun", "06") %>% str_replace("Jul", "07") %>% 
  str_replace("Aug", "08") %>% str_replace("Sep", "09") %>%
  str_replace("Oct", "10") %>% str_replace("Nov", "11") %>% str_replace("Dec", "12") 



starion$year_charge <- substr(starion$date_exp, 4,7)
starion$month_charge <- substr(starion$date_exp,1,2)
starion$year_commencement <- ""
starion$month_commencement <- ""
starion$zipcode <- starion$`Zip Code`
starion <- starion %>% select(-`Zip Code`)

str(starion)



# quick check dates
head(starion %>% select(`Month-Year` , month_charge, year_charge))
tail(starion %>% select(`Month-Year`, month_charge, year_charge))

# rename columns to match db table
str(starion)
starion <- starion %>% rename(supplier = `Supplier Name`,
                            billed_rate = Rate,
                            num_customers = `Number of Customers`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term in Months`,
                            servicechargesfees = `Service Charges or Other Fees`,
                            term_fee = `Termination Fee`,
                            num_terminations = `# of Terminations`)
colnames(starion) <- tolower(colnames(starion))


# select correct columns
starion <- starion %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)
unique(starion$contractterm)
starion$contractterm <- 99
starion$term_fee <-0

#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'starion'), value = starion, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.starion")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading starion.")



