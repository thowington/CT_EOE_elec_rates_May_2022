# load ACS majority-minority data

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
dbase <- perms$get("this_database")

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = "5432",
  user = user1,
  password = password1,
  dbname = dbase
)


# create table ----
dbSendQuery(con, "drop table if exists acs.majority_minority")

dbSendQuery(con, "create table acs.majority_minority (
  geography	varchar(15),
  geographic_area_name varchar(15),
  white_alone decimal(3,1),
  pct_minority decimal(4,3))
"
)


# pct minority table ----
input_file <- paste0(project_dir, "/ACS_data/ACSDP5Y2020.DP05-Data.xlsx")
inf <- read_excel(input_file, sheet = "pct_minority", skip = 2, col_names = FALSE)
head(inf)

colnames(inf) <- c("geography","geographic_area_name","white_alone","pct_minority")

inf <- inf %>%  mutate(zcta = substr(geographic_area_name, 7,11)) %>%
  mutate(pct_minority = round(pct_minority,3))

dbWriteTable(con, name = Id(schema = 'acs', table = 'majority_minority'), value = inf, overwrite = TRUE, row.names = FALSE)

table_for_map <- inf %>% select(zcta, pct_minority)
outfile = paste0(project_dir,"maps/minority_table.csv")
write.csv(table_for_map,outfile, row.names = FALSE)

# the overall percent minority in CT is 34%
zcta_gt_50_pct_minority <- inf %>% mutate(majority_minority = ifelse(pct_minority>=.5, 1, 0))
filename = paste0(project_dir, "/output/zcta_gt_50_pct_minority.csv")
write.csv(zcta_gt_50_pct_minority, filename, row.names = FALSE)

# the overall percent minority in CT is 34% - see ACS table DP05_0077PE
zcta_gt_ave_pct_minority <- inf %>% mutate(majority_minority = ifelse(pct_minority>=.34, 1, 0))
filename = paste0(project_dir, "/output/zcta_gt_ave_pct_minority.csv")
write.csv(zcta_gt_ave_pct_minority, filename, row.names = FALSE)


#  grouping by gt and lt CT ave minority repr ----
# 2021 CT ave minority repr was 34%
res2021 <- dbSendQuery(con,
                                   "select sum(supplier_customers) as supplier_customers,
	sum(edc_customers) as edc_customers,
	sum(total_customers) as total_customers,
	sum(supplier_customers)*1.0/sum(total_customers) as participation_rate,
	'Less than ave minority' as demo_group
from final_products.participation_rate_by_year_zcta a
join acs.majority_minority b
on a.zcta = b.zcta
where year = '2021' and pct_minority < .34
union
select sum(supplier_customers) as supplier_customers,
	sum(edc_customers) as edc_customers,
	sum(total_customers) as total_customers,
	sum(supplier_customers)*1.0/sum(total_customers) as participation_rate,
	'Greater than ave minority' as demo_group
from final_products.participation_rate_by_year_zcta a
join acs.majority_minority b
on a.zcta = b.zcta
where year = '2021' and pct_minority >= .34
")
part_rate_2021_minority <- dbFetch(res2021)


filename = paste0(project_dir, "/output/part_rate_2021_gtlt_ave_minority.csv")
write.csv(part_rate_2021_minority, filename, row.names = FALSE)

print("finished!")

