# load ACS data total population by ZCTA

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
dbSendQuery(con, "drop table if exists acs.total_population")


# load population data   ----
input_file <- paste0(project_dir, "/ACS_data/ACSDP5Y2021.DP05-Data_total_population.csv")
inf <- read.csv(input_file)


inf <- inf %>%mutate(zcta = substr(Area, 7,11),
                     total_population = Total_population) %>%
  select(zcta, total_population)
head(inf)



dbWriteTable(con, name = Id(schema = 'acs', table = 'total_population'), value = inf, overwrite = TRUE, row.names = FALSE)


print("finished loading population data.")

