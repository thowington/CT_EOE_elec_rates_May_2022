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




#### spark
res <- dbSendQuery(con, "drop table if exists q6.spark")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.spark (
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
spark <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 4.xlsx",
                     sheet = "Spark",
                     skip = 0,
                     col_names = TRUE)
str(spark)

#  clean up dates and zip codes

spark$year_charge <- substr(spark$`Month-Year`, 1,4)
spark$month_charge <- substr(spark$`Month-Year`,6,7)
spark$year_commencement <- substr(spark$`ContractStartYearMonth`, 1,4)
spark$month_commencement <- substr(spark$`ContractStartYearMonth`, 6,7)
spark$zipcode <- paste0("0", spark$`Zip Code`)
spark <- spark %>% select(-`Zip Code`)

str(spark)



# quick check dates
head(spark %>% select(`Month-Year` , month_charge, year_charge))
tail(spark %>% select(`Month-Year`, month_charge, year_charge))

head(spark %>% select(`ContractStartYearMonth`,year_commencement , month_commencement))
tail(spark %>% select(`ContractStartYearMonth`,year_commencement , month_commencement))


# rename columns to match db table
str(spark)
spark <- spark %>% rename(supplier = `Supplier Name`,
                            billed_rate = `Billed Rate`,
                            num_customers = `Customer Count`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term`,
                            servicechargesfees = `Monthly Service Fee`,
                            term_fee = `Termination Amt.`,
                            num_terminations = `Termination Count`)
colnames(spark) <- tolower(colnames(spark))


# select correct columns
spark <- spark %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)


#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'spark'), value = spark, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.spark")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished.")



