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




#### ctg & e
dbSendQuery(con, "drop table if exists q6.ctge")

dbSendQuery(con, "create table q6.ctge (
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
ctge <- read_excel(paste0(project_dir, "/from_EOE/# Summarized EOE-6 Supplier Data/Supplier Summized Data - 2.xlsx"),
                     sheet = "CTG&E",
                     skip = 0,
                     col_names = TRUE)
#str(ctge)

#  clean up dates and zip codes
ctge$year_charge <- substr(ctge$`Month-Year`, 1,4)
ctge$month_charge <- substr(ctge$`Month-Year`,6,7)
ctge$year_commencement <- substr(ctge$`Current Contract Commencement Date (month year)`, 1,4)
ctge$month_commencement <- substr(ctge$`Current Contract Commencement Date (month year)`, 6,7)
ctge$zipcode <- paste0("0", as.character(ctge$`Zip Code`))
ctge <- ctge %>% select(-`Zip Code`)
ctge$contractterm <- as.integer(ctge$`Contract Term in Months (i.e., length of contract)`)

#str(ctge)


# quick check dates
head(ctge %>% select(`Month-Year`,year_charge , month_charge))
tail(ctge %>% select(`Month-Year`,year_charge , month_charge))

head(ctge %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))
tail(ctge %>% select(`Current Contract Commencement Date (month year)`,year_commencement , month_commencement))



# rename columns to match db table
colnames(ctge) <- tolower(colnames(ctge))


ctge <- ctge %>% rename(supplier =`supplier name`,
                            billed_rate = rate,
                            num_customers = `number of customers`,
                            totalkwh = `total kwh`,
                            servicechargesfees = `service charge or other fees (excluding termination fees)`,
                            term_fee = `termination fees`,
                            num_terminations = `# of terminations`)

ctge <- ctge %>% select(supplier, year_charge, month_charge,
                            billed_rate, zipcode, num_customers,
                            totalkwh, year_commencement, month_commencement,
                            contractterm, servicechargesfees, term_fee,
                            num_terminations, edc)

dbWriteTable(con, name = Id(schema = 'q6', table = 'ctge'), value = ctge, append = TRUE, row.names = FALSE)

#check totals
res <- dbSendQuery(con, "select count(*) rec_count,
                          sum(billed_rate) sum_billedrate,
	                        sum(num_customers) sum_customers,
	                        sum(totalkwh) sum_totalkwh,
	                        sum(contractterm) sum_contractterm
	                        from q6.ctge")
result <- dbFetch(res)
print(result)
# dbClearResult(con)
dbDisconnect(con)
print("finished loading ctge.")



