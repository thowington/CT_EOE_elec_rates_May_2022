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

dbSendQuery(con, "drop table if exists q6.all;") 
dbSendQuery(con, "create table q6.all as select * from q6.ambit;") 

dbSendQuery(con, "insert into q6.all select * from q6.atlantic;") 
dbSendQuery(con, "insert into q6.all select * from q6.clearview;") 
dbSendQuery(con, "insert into q6.all select * from q6.cne;") 
dbSendQuery(con, "insert into q6.all select * from q6.ctge;") 
dbSendQuery(con, "insert into q6.all select * from q6.direct;") 
dbSendQuery(con, "insert into q6.all select * from q6.energyplus;") 
dbSendQuery(con, "insert into q6.all select * from q6.er;") 
dbSendQuery(con, "insert into q6.all select * from q6.major;") 
dbSendQuery(con, "insert into q6.all select * from q6.mega;") 
dbSendQuery(con, "insert into q6.all select * from q6.nap;") 
dbSendQuery(con, "insert into q6.all select * from q6.nge;") 
dbSendQuery(con, "insert into q6.all select * from q6.public;") 
dbSendQuery(con, "insert into q6.all select * from q6.reliant;") 
dbSendQuery(con, "insert into q6.all select * from q6.residents;") 
dbSendQuery(con, "insert into q6.all select * from q6.spark;") 
dbSendQuery(con, "insert into q6.all select * from q6.starion;") 
dbSendQuery(con, "insert into q6.all select * from q6.think;") 
dbSendQuery(con, "insert into q6.all select * from q6.townsquare;") 
dbSendQuery(con, "insert into q6.all select * from q6.verde;") 
dbSendQuery(con, "insert into q6.all select * from q6.viridian;") 
dbSendQuery(con, "insert into q6.all select * from q6.wattifi;") 
dbSendQuery(con, "insert into q6.all select * from q6.xoom;")

# cleanup
dbSendQuery(con, "delete from q6.all
where billed_rate is NULL
or totalkwh is NULL")

dbSendQuery(con, "delete from q6.all
where totalkwh < 0")

dbSendQuery(con, "delete from q6.all
where billed_rate <0")

# remove non-CT zips
dbSendQuery(con, "delete from q6.all
where zipcode not like '06%'")

dbSendQuery(con, "delete from q6.all
where length(zipcode) !=5")


# make edc names uniform

dbSendQuery(con, "update q6.all
set edc = 'UI'
where edc in ('UIC','United Illum','United Illuminating','UI');")

dbSendQuery(con, "update q6.all
set edc = 'EV'
where edc in ('Eversource (CL&P)','Eversource Energy - CT',
              'Connecticut Light & Power','Connecticut Light and Power','Eversource',
              'CL&P','CLP');")

print("finished aggregating q6 data.")