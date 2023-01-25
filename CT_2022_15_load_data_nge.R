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




#### nge
res <- dbSendQuery(con, "drop table if exists q6.nge")
dbClearResult(res)

res <- dbSendQuery(con, "create table q6.nge (
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
nge <- read_excel(paste0(project_dir, "/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 3.xlsx"),
                     sheet = "NGE",
                     skip = 0,
                     col_names = TRUE)
str(nge)

#  clean up dates and zip codes
nge$year_charge <- paste0("20",substr(nge$`Month-Year`, str_locate(nge$`Month-Year`,"/")+1, nchar(nge$`Month-Year`)))
nge$month_charge <- paste0("0",substr(nge$`Month-Year`,1,str_locate(nge$`Month-Year`,"/")-1))
nge$month_charge <- nge$month_charge %>% str_replace("010","10") %>% str_replace("011","11") %>% str_replace("012","12")

nge$year_commencement <- paste0("20", substr(nge$`Current Contract Commencement Date (month year)`,
                                             str_locate(nge$`Current Contract Commencement Date (month year)`, "/")+1, 
                                             nchar(nge$`Current Contract Commencement Date (month year)`)))
nge$month_commencement <- paste0("0",substr(nge$`Current Contract Commencement Date (month year)`,1,
                                            str_locate(nge$`Current Contract Commencement Date (month year)`,"/")-1))
nge$month_commencement <- nge$month_commencement %>% str_replace("010","10") %>% str_replace("011","11") %>% str_replace("012","12")

nge$zipcode <- paste0("0", nge$`Zip Code`)
nge <- nge %>% select(-`Zip Code`)



str(nge)



# quick check dates
head(nge %>% select(`Month-Year` , month_charge, year_charge))
tail(nge %>% select(`Month-Year`, month_charge, year_charge))

head(nge %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))
tail(nge %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))


# rename columns to match db table
str(nge)
nge <- nge %>% rename(supplier = `Supplier Name`,
                            billed_rate = Rate,
                            num_customers = `Number of Customers`,
                            totalkwh = `Total kWh`,
                            contractterm = `Contract Term in Months`,
                            servicechargesfees = `Service Charges of Other Fees`,
                            term_fee = `Termination Fee` ,
                            num_terminations = `# of Terminations`)
colnames(nge) <- tolower(colnames(nge))



# select correct columns
nge <- nge %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

# contractterm has some NAs.  Here I assume that these contracts are month-to-month.
nge$contractterm <- nge$contractterm %>% str_replace("NA","1")
nge$contractterm <- as.integer(nge$contractterm)


#load
try(
dbWriteTable(con, name = Id(schema = 'q6', table = 'nge'), value = nge, append = TRUE, row.names = FALSE),
silent = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count, 
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.nge")
result <- dbFetch(res)
print(result)

dbDisconnect(con)
print("finished loading nge.")



