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
)

dbSendQuery(con, "drop database CT_2022")
dbSendQuery(con, "create database CT_2022")
dbDisconnect(con)

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = "5432",
  user = user1,
  password = password1,
  dbname = "ct_2022"
)

dbSendQuery(con, "create schema q6")
dbSendQuery(con, "create schema analysis")
dbSendQuery(con, "create schema final_products")
dbSendQuery(con, "create schema edc_data")
dbSendQuery(con, "create schema q7")
dbDisconnect(con)
