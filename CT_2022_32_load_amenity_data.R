# load raw data from responses to EOE-7 (amenities)


library(RPostgres)
library(odbc)
library(dplyr)
library(readxl)
library(ConfigParser)
library(stringr)
library(ggplot2)
library(data.table)


config_file<- "C:/Users/thowi/Documents/consulting_work/DO_NOT_SHARE/CT_2022_config_file.ini"
config_parameters <- ConfigParser$new()
perms <- config_parameters$read(config_file)
user1 <- perms$get("user")
password1 <- perms$get("password")
project_dir <- perms$get("project_dir")

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = "5432",
  user = user1,
  password = password1,
  dbname = "ct_2022"
)


# create table ----
dbSendQuery(con, "drop table if exists q7.all")

dbSendQuery(con, "create table q7.all (
  supplier varchar(30),
  year  char(4),
  amenity varchar(40),
  alleged_value decimal(6,2),
  num_redeemed  decimal(10,2),
  total_value_redeemed decimal(15,2))"
)


# Ambit ----
input_file <- paste0(project_dir, "supplier_responses/Ambit/2022.07.19-Ambit Interrogatory.xlsx")
inf <- read_excel(input_file, sheet = "Table 2 (EOE-7)", skip = 4, col_names = TRUE)
inf <- inf[-c(1,2),]  # remove one row of blank/NA values an done with false header names
#colnames(inf)
#inf

inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenities Redeemed by Customers`) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed) %>%
  mutate(alleged_value= as.numeric(alleged_value),
         num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = alleged_value*num_redeemed)

dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)

# Atlantic ----
input_file <- paste0(project_dir, "supplier_responses/Atlantic/Atlantic_18-06-02RE01_Interrogatory EOE -7, Attachment A (CONFIDENTIAL).xlsx")
inf <- read_excel(input_file, sheet = "Sheet1", skip = 0, col_names = TRUE)
# colnames(inf)
# inf
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Amount of Amenity Distributed to Customers`) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed) %>%
  mutate(alleged_value= as.numeric(alleged_value),
         num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = alleged_value*num_redeemed)

#sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)


# Clearview ----
input_file <- paste0(project_dir, "supplier_responses/Clearview/Clearview Electric, Inc. EOE-7 Table 2 (1).xlsx")
inf <- read_excel(input_file, sheet = "EOE-7 Amenities ", skip = 1, col_names = TRUE)
# colnames(inf)
# inf
inf <- inf %>% rename(supplier = `SUPPLIER NAME`,
                      year = `YEAR`,
                      amenity = `AMENITY OFFERED`,
                      alleged_value = `VALUE OF AMENITY`,
                      num_redeemed = `AMOUNT OF AMENITY DISTRIBUTED TO CUSTOMERS`) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed)

#  correct hard to read records
inf[which(inf$amenity == '$75 and ongoing free weekend charging (up to 250kwh)'),]$alleged_value <- "75"
inf[which(inf$amenity == 'Hulu subscription'),]$alleged_value <- "5.99"
inf[which(inf$amenity == '5% of 12 month kwh expenditure' & inf$year == '2019'),]$alleged_value <- "66.80"
inf[which(inf$amenity == '5% of 12 month kwh expenditure' & inf$year == '2020'),]$alleged_value <- "54.00"
inf[which(inf$amenity == '5% of 12 month kwh expenditure' & inf$year == '2020'),]$num_redeemed <- 270
inf[which(inf$amenity == '5% of 12 month kwh expenditure' & inf$year == '2021'),]$alleged_value <- "45.31"
inf[which(inf$amenity == '1USB Battery Bank'),]$year = "2018"
inf[which(inf$amenity == '8 LED Bulbs and 8 LED Bulbs'),]$year = "2018"
inf$supplier <- 'Clearview Energy'
inf$year <- substr(inf$year,1,4)
inf$amenity <- substr(inf$amenity, 1,40)
inf <- inf %>%  mutate(alleged_value= as.numeric(alleged_value),
         num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = alleged_value*num_redeemed)
### have to choose correct cols
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)



# Constellation ----
input_file <- paste0(project_dir, "supplier_responses/Constellation/CNE EOE-7 Attachment A.xlsx")
inf <- read_excel(input_file, sheet = "07", skip = 1, col_names = TRUE)
# inf
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Provided`,
                      alleged_value = `Value ($)`,
                      num_redeemed = `Amentities Redeemed`) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed) %>%
  mutate(alleged_value= as.numeric(alleged_value),
         num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = alleged_value*num_redeemed)

dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)

# CTG&E (UGE) ----
input_file <- paste0(project_dir, "supplier_responses/CTGE/2022.09.13 CTGE Table EOE-29 (Perks) Exh. EOE-29, A, CT PURA Docket No. 18-06-02RE01.xlsx")
inf <- read_excel(input_file, sheet = "Table 2 (EOE-7)", skip = 3, col_names = TRUE)
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenities Redeemed by Customers`) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed) %>%
  mutate(alleged_value= as.numeric(alleged_value),
         num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = alleged_value*num_redeemed)
#sum(inf$total_value_redeemed)

dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)

#Direct Energy ----
input_file <- paste0(project_dir, "supplier_responses/DES/DES_Interrogatory EOE-7 Attachment B.xlsx")
inf <- read_excel(input_file, sheet = "Sheet1", skip = 0, col_names = TRUE)
# colnames(inf)
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenities Redeemed by Customer`) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed) %>%
  filter(num_redeemed != "NULL") %>%
  mutate(alleged_value= as.numeric(alleged_value),
         num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = alleged_value*num_redeemed)
#sum(inf$total_value_redeemed)
inf$amenity <- substr(inf$amenity, 1, 40)
inf$supplier <- "Direct Energy Services, LLC"
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)

# Energy Plus ----
input_file <- paste0(project_dir, "supplier_responses/EPH/EPH EOE-30 Att A.XLSX")
inf <- read_excel(input_file, sheet = "EOE-30 Att A", skip = 0, col_names = TRUE)
#colnames(inf)
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenities Redeemed by Customers`,
                      total_value_redeemed = `Cost of Miles/Points Redeemed`) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed) 

# correct for some records based on x% cash back
inf <- inf %>% mutate(total_value_redeemed = ifelse(is.na(total_value_redeemed) == TRUE, num_redeemed, total_value_redeemed),
                      alleged_value = NULL)

sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)

# Energy Rewards / Everyday Energy
input_file <- paste0(project_dir, "supplier_responses/ER/2022.07.19- Energy Rewards Interrogatory Attachments.xlsx")
inf <- read_excel(input_file, sheet = "Table 2 (EOE-7)", skip = 4, col_names = TRUE)
#inf
#colnames(inf)
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenities Redeemed by Customers`) %>%
  mutate(alleged_value= as.numeric(alleged_value),
       num_redeemed = as.numeric(num_redeemed),
       total_value_redeemed = alleged_value*num_redeemed) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed) 
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)


# North American Power ----
input_file <- paste0(project_dir, "supplier_responses/NAP/2022.08.03- North American Power Interrogatory Attachment EOE-7, EOE-9, EOE-12 , CT PURA Docket No. 18-06-02RE01.xlsx.xlsx")
inf <- read_excel(input_file, sheet = "EOE-7 (Table 2)", skip = 3, col_names = TRUE)
# inf
# colnames(inf)
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Amount of Amenity Distributed to Customers`) %>%
  filter(amenity != "Understandabill") %>%
  mutate(alleged_value= as.numeric(alleged_value),
         num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = alleged_value*num_redeemed) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed) 
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)


# Public Power ----
input_file <- paste0(project_dir, "supplier_responses/PP/PP_2 Supplemental PP EOE Tables 7.22.22.xlsx")
inf <- read_excel(input_file, sheet = "Table 2 (EOE-7)", skip = 4, col_names = TRUE)
# inf
# colnames(inf)
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenities Redeemed by Customers`) %>%
  mutate(alleged_value= as.numeric(alleged_value),
         num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = alleged_value*num_redeemed) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed)
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)


# Reliant ----
# For this one I assume that the number_redeemed equals the total_value_redeemed.  

input_file <- paste0(project_dir, "supplier_responses/Reliant/REN_Interrogatory EOE-7 Att. A.XLSX")
inf <- read_excel(input_file, sheet = "EOE-7A", skip = 0, col_names = TRUE)
# colnames(inf)
# inf
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenities Redeemed by Customers`) %>%
  mutate(alleged_value = substr(alleged_value, 1,3),
         num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = as.numeric(num_redeemed)) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed)
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)


# Town Square 2018 ----
# note data is reported on separate sheets.
input_file <- paste0(project_dir, "supplier_responses/TownSquare/18-06-02RE01 - Exhibit EOE-7 - Town Square Energy  CT Amenities - Table 2 - 07.19.2022.xlsx")
inf <- read_excel(input_file, sheet = "2018", skip = 1, col_names = TRUE)
colnames(inf)
inf
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenties Redeemed by Customers`) %>%
  mutate(num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = alleged_value*num_redeemed) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed)
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)

# Town Square 2019 ----
# note data is reported on separate sheets.
input_file <- paste0(project_dir, "supplier_responses/TownSquare/18-06-02RE01 - Exhibit EOE-7 - Town Square Energy  CT Amenities - Table 2 - 07.19.2022.xlsx")
inf <- read_excel(input_file, sheet = "2019", skip = 1, col_names = TRUE)
colnames(inf)
inf
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenties Redeemed by Customers`) %>%
  mutate(num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = alleged_value*num_redeemed) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed)
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)


# Town Square 2020 ----
# note data is reported on separate sheets.
# merged cells cause problems.  I have to simplify.
input_file <- paste0(project_dir, "supplier_responses/TownSquare/18-06-02RE01 - Exhibit EOE-7 - Town Square Energy  CT Amenities - Table 2 - 07.19.2022.xlsx")
inf <- read_excel(input_file, sheet = "2020", skip = 1, col_names = TRUE)
colnames(inf)
inf
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenties Redeemed by Customers`,
                      total_value_redeemed = `Amount of Amenity Distributed to Customers`) %>%
  mutate(num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = as.numeric(total_value_redeemed)) %>%
  replace(is.na(.), 0) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed)
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)


# Town Square 2021 ----
input_file <- paste0(project_dir, "supplier_responses/TownSquare/18-06-02RE01 - Exhibit EOE-7 - Town Square Energy  CT Amenities - Table 2 - 07.19.2022.xlsx")
inf <- read_excel(input_file, sheet = "2021", skip = 1, col_names = TRUE)
colnames(inf)
inf
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenties Redeemed by Customers`,
                      total_value_redeemed = `Amount of Amenity Distributed to Customers`) %>%
  mutate(num_redeemed = as.numeric(num_redeemed),
         total_value_redeemed = as.numeric(total_value_redeemed)) %>%
  replace(is.na(.), 0) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed)
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)


# Verde ----
# Note that some amenities are reward points, not rebates.  Verde correctly calculated the values redeemed.
input_file <- paste0(project_dir, "supplier_responses/Verde/Verde_CT 18--06-02RE01_EOE-31 Att A.XLS")
inf <- read_excel(input_file, sheet = "Sheet1", skip = 0, col_names = TRUE)
colnames(inf)
inf
inf <- inf %>% rename(supplier = `Supplier Name`,
                      year = `Year`,
                      amenity = `Amenity Offered`,
                      alleged_value = `Value of Amenity`,
                      num_redeemed = `Number of Amenities Redemeed by Customers`,
                      total_value_redeemed = `Cost to Company`) %>%
  mutate(num_redeemed = as.numeric(num_redeemed),
         alleged_value = as.numeric(substr(alleged_value, 1,3))) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed)
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)


# Xoom 1 ----
#  Note there are four different sheets to input.
input_file <- paste0(project_dir, "supplier_responses/Xoom/XOOM_Interrogatory EOE-7 Attachment B.XLSX")
inf <- read_excel(input_file, sheet = "EOE-7b XOOM Xtras", skip = 0, col_names = TRUE)
colnames(inf)
inf
inf <- inf %>% rename(supplier = `i. Supplier Name;`,
                      year = `ii. Year (not date) of amenity provided;`,
                      amenity = `iii. The type of amenity offered (i.e. gift card, customer credit, airline mileage, energy efficient perks, etc);`,
                      alleged_value = `vi. the value of each amenity offered;`,
                      num_redeemed = `viii. how many of each amenity was redeemed.`) %>%
  mutate(total_value_redeemed = alleged_value* num_redeemed) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed)
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)


# Xoom 2 ----
#  Note there are four different sheets to input.
input_file <- paste0(project_dir, "supplier_responses/Xoom/XOOM_Interrogatory EOE-7 Attachment B.XLSX")
inf <- read_excel(input_file, sheet = "EOE-7b XOOM Online Rewards", skip = 0, col_names = TRUE)
colnames(inf)
inf
inf <- inf %>% rename(supplier = `i. Supplier Name;`,
                      year = `ii. Year (not date) of amenity provided;`,
                      amenity = `iii. The type of amenity offered (i.e. gift card, customer credit, airline mileage, energy efficient perks, etc);`,
                      alleged_value = `vi. the value of each amenity offered;`,
                      num_redeemed = `viii. how many of each amenity was redeemed.`) %>%
  mutate(total_value_redeemed = alleged_value* num_redeemed) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed)
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)


# Xoom 3 ----
#  Note no monetary value of airline miles was provided.  
input_file <- paste0(project_dir, "supplier_responses/Xoom/XOOM_Interrogatory EOE-7 Attachment B.XLSX")
inf <- read_excel(input_file, sheet = "EOE-7b XOOM Airline Miles", skip = 0, col_names = TRUE)
colnames(inf)
inf


# Xoom 4 ----
#  Note there are four different sheets to input.
input_file <- paste0(project_dir, "supplier_responses/Xoom/XOOM_Interrogatory EOE-7 Attachment B.XLSX")
inf <- read_excel(input_file, sheet = "EOE-7b XOOM Charitable Donation", skip = 0, col_names = TRUE)
colnames(inf)
inf
inf <- inf %>% rename(supplier = `i. Supplier Name;`,
                      year = `ii. Year (not date) of amenity provided;`,
                      amenity = `iii. The type of amenity offered (i.e. gift card, customer credit, airline mileage, energy efficient perks, etc);`,
                      alleged_value = `vi. the value of each amenity offered;`,
                      num_redeemed = `viii. how many of each amenity was redeemed.`) %>%
  mutate(total_value_redeemed = alleged_value) %>%
  select(supplier, year, amenity, alleged_value, num_redeemed, total_value_redeemed)
sum(inf$total_value_redeemed)
dbWriteTable(con, name = Id(schema = 'q7', table = 'all'), value = inf, append = TRUE, row.names = FALSE)

print("finished loading amenity data.")

# remove 2022 data ----
dbSendQuery(con,"delete from q7.all where year not in ('2017','2018','2019','2020','2021')")


# make company names same as in Q6 ----

dbSendQuery(con, "update q7.all set supplier = 'Constellation NewEnergy Inc'
            where supplier = 'Constellation'")
dbSendQuery(con, "update q7.all set supplier = 'XOOM'
            where supplier = 'XOOM Energy'")
dbSendQuery(con, "update q7.all set supplier = 'ClearView Electric'
            where supplier = 'Clearview Energy'")
dbSendQuery(con, "update q7.all set supplier = 'Energy Plus'
            where supplier = 'energyplus'")

#  analysis summary by year ----
res1 <- dbSendQuery(con,"select * from 
(with val_redeemed as (
  select year, sum(total_value_redeemed) value_redeemed 
  	from q7.all
  group by year
  order by year asc
),
bills as (
  select year_charge, 
    sum(num_customers) as total_bills,
    sum(overpayment) as overpayment
  from final_products.rate_comparison
  group by year_charge
  order by year_charge asc
	)
  select year, 
    TO_CHAR(value_redeemed, 'fm$999,999,999') as \"value_redeemed\",
    TO_CHAR(total_bills, 'fm999,999,999') as \"total_bills\",
    TO_CHAR(overpayment, 'fm$999,999,999') as \"total_overpayment\",
    TO_CHAR(value_redeemed/total_bills, 'fm$9.99') as \"value_redeemed_per_bill\",
    TO_CHAR(100*value_redeemed/overpayment,'99D9%') as \"value/overpayment\"
  from val_redeemed a
  join bills b
  on a.year = b.year_charge) a
                    
union

select * from (
with val_redeemed as (
  select year, 
    sum(total_value_redeemed) value_redeemed 
  from q7.all
  group by year
  order by year asc
),
bills as (
  select year_charge, 
    sum(num_customers) as total_bills,
    sum(overpayment) as overpayment
  from final_products.rate_comparison
  group by year_charge
  order by year_charge asc
	)
select 'Total' as year, 
  TO_CHAR(sum(value_redeemed), 'fm$999,999,999') as \"value_redeemed\", 
  TO_CHAR(sum(total_bills), 'fm999,999,999') as \"total_bills\",
  TO_CHAR(sum(overpayment), 'fm$999,999,999') as \"overpayment\",
  TO_CHAR(sum(value_redeemed)/sum(total_bills), 'fm$9.99') as \"value_redeemed_per_bill\",
  TO_CHAR(100*sum(value_redeemed)/sum(overpayment),'fm99D99%') as \"val/overpayment\"
from val_redeemed a
join bills b
on a.year = b.year_charge
	) b
	
order by year
")
amenity_table <- dbFetch(res1)
amenity_table

filename = paste0(project_dir,"output/","amenity_summary_by_year.csv")
write.csv(amenity_table, filename, row.names = FALSE)


# analysis summary by supplier ----

res2 <- dbSendQuery(con,"(with val_redeemed as (
  select supplier, sum(total_value_redeemed) value_redeemed 
  from q7.all
  group by supplier
  order by supplier asc
),
bills as (
  select supplier, 
  sum(num_customers) as total_bills,
  sum(overpayment) as overpayment
  from final_products.rate_comparison
  group by supplier
  order by supplier asc
)
select b.supplier, 
TO_CHAR(value_redeemed, '$99,999,999') as \"value_redeemed\",
    TO_CHAR(total_bills, '9,999,999') as \"total_bills\",
    TO_CHAR(overpayment, '$999,999,999') as \"total_overpayment\",
    TO_CHAR(value_redeemed/total_bills, '$99.99') as \"value_redeemed_per_bill\",
	TO_CHAR(100*value_redeemed/overpayment,'999D99%') as \"val/overpayment\"
from val_redeemed a
 right join bills b
on a.supplier = b.supplier)")
amenity_table_by_supplier <- dbFetch(res2)
amenity_table_by_supplier[is.na(amenity_table_by_supplier)] <- 0 
amenity_table_by_supplier

res3 <- dbSendQuery(con, "with val_redeemed as (
select 'All' as supplier, sum(total_value_redeemed) value_redeemed 
	from q7.all
),
bills as (
select 'All' as supplier, 
sum(num_customers) as total_bills,
sum(overpayment) as overpayment
from final_products.rate_comparison
	)
select 'All' as supplier, 
  	TO_CHAR(sum(value_redeemed), '$99,999,999') as \"value_redeemed\", 
  	TO_CHAR(sum(total_bills), '99,999,999') as \"total_bills\",
  	TO_CHAR(sum(overpayment), '$999,999,999') as \"total_overpayment\",
  	TO_CHAR(sum(value_redeemed)/sum(total_bills), '$9.99') as \"value_redeemed_per_bill\",
	TO_CHAR(100*sum(value_redeemed)/sum(overpayment),'99D99%') as \"val/overpayment\"
from val_redeemed a
join bills b
on a.supplier = b.supplier")

amenity_table_by_supplier_all <- dbFetch(res3)
amenity_table_by_supplier_all

# combine the two parts
amentity_table_by_supplier_final <- rbind(amenity_table_by_supplier, amenity_table_by_supplier_all)

amentity_table_by_supplier_final

filename = paste0(project_dir,"output/","amenity_table_by_supplier.csv")
write.csv(amentity_table_by_supplier_final, filename, row.names = FALSE)




print("finished running amenities script.")

