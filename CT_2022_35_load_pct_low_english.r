# load ACS data percent of households with low proficiency English

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
dbSendQuery(con, "drop table if exists acs.pct_low_english")

dbSendQuery(con, "create table acs.pct_low_english (
  ZCTA char(5),
  Households integer,
  Limited_English_Households integer,
  pct_low_english decimal(4,3))
"
)


# load pct low english data----
input_file <- paste0(project_dir, "/ACS_data/ACSST5Y2021.S1602-Data.csv")
inf <- read_csv(input_file)
head(inf)

colnames(inf) <- c("zcta","households","limited_english_households","pct_low_english")


dbWriteTable(con, name = Id(schema = 'acs', table = 'pct_low_english'), value = inf, overwrite = TRUE, row.names = FALSE)


#  summarize zctas with low english ----
res_low_eng <- dbSendQuery(con, "select distinct a.zcta, a.median_hh_income,
	b.pct_minority,
	c.pct_low_english,
	d.po_name
from acs.income a
join acs.majority_minority b
on a.zcta = b.zcta
join acs.pct_low_english c
on a.zcta = c.ZCTA
join acs.crosswalk d
on a.zcta = d.zcta
where c.pct_low_english > 0.1")
low_english_zctas <- dbFetch(res_low_eng)

filename = paste0(project_dir, "/output/low_english_zctas_summary.csv")
write_csv(low_english_zctas, filename)

# summarize high participation rate zctas ----

res_high_part <- dbSendQuery(con, "select distinct a.zcta, 
	d.po_name,
	e.participation_rate,
	e.supplier_customers,
	a.median_hh_income,
	b.pct_minority,
	c.pct_low_english
from acs.income a
join acs.majority_minority b
on a.zcta = b.zcta
join acs.pct_low_english c
on a.zcta = c.ZCTA
join acs.crosswalk d
on a.zcta = d.zcta
join final_products.participation_rate_by_year_zcta e
on a.zcta = e.zcta
where e.participation_rate > 0.20
and e.year = '2021'
and a.zcta != '06020'
order by e.participation_rate desc
")
high_participation_zctas <- dbFetch(res_high_part)

filename = paste0(project_dir, "/output/high_participation_zctas_summary.csv")
write_csv(high_participation_zctas, filename)

