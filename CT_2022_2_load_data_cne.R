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




#### CNE - Constellation NewEnergy
dbSendQuery(con, "drop table if exists q6.cne")
dbSendQuery(con, "create table q6.cne (
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

# read raw data
cne <- read_excel("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 1.xlsx",
                     sheet = "CNE",
                     skip = 0,
                     col_names = TRUE)
str(cne)

#  clean up dates and zip codes
cne$Year_Charge <- substr(cne$MonthYear_ResCharge, 1,4)
cne$Month_Charge <- substr(cne$MonthYear_ResCharge,6,8)
cne$Year_CommencementDate <- substr(cne$MonthYear_CommencementDate, 1,4)
cne$Month_CommencementDate <- substr(cne$MonthYear_CommencementDate, 6,8)
cne$zipcode <- paste0("0", as.character(cne$ZipCode))
cne <- cne %>% select(-ZipCode)

str(cne)


# quick check dates
head(cne %>% select(MonthYear_ResCharge,Year_Charge , Month_Charge))
tail(cne %>% select(MonthYear_ResCharge,Year_Charge , Month_Charge))

head(cne %>% select(MonthYear_CommencementDate,Year_CommencementDate , Month_CommencementDate))
tail(cne %>% select(MonthYear_CommencementDate,Year_CommencementDate , Month_CommencementDate))


# rename columns to match db table
colnames(cne) <- tolower(colnames(cne))
cne <- cne %>% rename(supplier = suppliername,
                            billed_rate = billedrate,
                            num_customers = numberofcustomer_rate_zip,
                            year_commencement = year_commencementdate,
                            month_commencement = month_commencementdate,
                            term_fee = contractterminationfee,
                            num_terminations = actualterminationfee,
                            edc = ldc)

cne <- cne %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

dbWriteTable(con, name = Id(schema = 'q6', table = 'cne'), value = cne, append = TRUE, row.names = FALSE)

#check totals
res <- dbSendQuery(con, "select sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm,
	                        count(*) rec_count
	                        from q6.cne")
result <- dbFetch(res)
result
# dbClearResult(con)
# dbDisconnect(con)

print("finished loading cne.")



