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
dbSendQuery(con, "drop schema if exists q6 cascade")
dbSendQuery(con, "create schema q6")
dbSendQuery(con, "drop schema if exists analysis cascade")
dbSendQuery(con, "create schema analysis")
dbSendQuery(con, "drop schema if exists final_products cascade")
dbSendQuery(con, "create schema final_products")
dbSendQuery(con, "drop schema if exists edc_data cascade")
dbSendQuery(con, "create schema edc_data")
dbSendQuery(con, "drop schema if exists q7 cascade")
dbSendQuery(con, "create schema q7")
dbDisconnect(con)
