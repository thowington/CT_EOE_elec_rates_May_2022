library(RPostgres)
library(odbc)
library(dplyr)
library(readxl)
library(ConfigParser)
library(stringr)
library(readr)
library(reshape2)

config_file<- "C:/Users/thowi/Documents/consulting_work/DO_NOT_SHARE/CT_2022_config_file.ini"
config_parameters <- ConfigParser$new()
perms <- config_parameters$read(config_file)
user1 <- perms$get("user")
password1 <- perms$get("password")
project_dir <- perms$get("project_dir")
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

res1 <- dbSendQuery(con, "select contractterm, month_commencement, sum(overpayment) 
from final_products.rate_comparison
where contractterm > 11 and contractterm <37
and edc = 'EV'
group by contractterm, month_commencement
order by contractterm, month_commencement")
res_ev <- dbFetch(res1)
str(res_ev)

ev_12_36 <- dcast(res_ev,  month_commencement~contractterm, mean)
filename = paste0(project_dir, "/output/EV_contractterm_12_36.csv")
write.csv(ev_12_36, filename, row.names = FALSE)


res2 <- dbSendQuery(con, "select sum(overpayment) 
from final_products.rate_comparison
where (contractterm <12 or contractterm is null)
and edc = 'EV'
")
res_ev2 <- dbFetch(res2)
res_ev2

res3 <- dbSendQuery(con, "select sum(overpayment) 
from final_products.rate_comparison
where contractterm >36
and edc = 'EV'")
res_ev3 <- dbFetch(res3)
res_ev3







res4 <- dbSendQuery(con, "select contractterm, month_commencement, sum(overpayment) 
from final_products.rate_comparison
where contractterm > 11 and contractterm <37
and edc = 'UI'
group by contractterm, month_commencement
order by contractterm, month_commencement")
res_ui <- dbFetch(res4)
str(res_ui)

ui_12_36 <- dcast(res_ui,  month_commencement~contractterm, mean)


filename = paste0(project_dir, "/output/UI_contractterm_12_36.csv")
write.csv(ui_12_36, filename, row.names = FALSE)



res5 <- dbSendQuery(con, "select sum(overpayment) 
from final_products.rate_comparison
where (contractterm <12 or contractterm is null)
and edc = 'UI'
")
res_ui2 <- dbFetch(res5)
res_ui2

res6 <- dbSendQuery(con, "select sum(overpayment) 
from final_products.rate_comparison
where contractterm >36
and edc = 'UI'")
res_ui3 <- dbFetch(res6)
res_ui3

## checking totals ----
check1 <- dbSendQuery(con, "
select sum(overpayment), sum(num_customers)
from final_products.rate_comparison
where edc = 'EV'
and (contractterm < 12 or contractterm is NULL)
                      ")
res_ch1 <- dbFetch(check1)

check2 <- dbSendQuery(con, "
select sum(overpayment), sum(num_customers)
from final_products.rate_comparison
where edc = 'EV'
and contractterm > 11 and contractterm < 37                      
                      ")
res_ch2 <- dbFetch(check2)

check3 <- dbSendQuery(con, "
select sum(overpayment), sum(num_customers)
from final_products.rate_comparison
where edc = 'EV'
and contractterm > 36                     
                      ")
res_ch3 <- dbFetch(check3)

check4 <- dbSendQuery(con, "
select sum(overpayment) , sum(num_customers)
from final_products.rate_comparison
where edc = 'UI'
and (contractterm < 12 or contractterm is NULL)                      
                      ")
res_ch4 <- dbFetch(check4)

check5 <- dbSendQuery(con, "
select sum(overpayment) , sum(num_customers)
from final_products.rate_comparison
where edc = 'UI'
and contractterm > 11 and contractterm < 37                      
                      ")
res_ch5 <- dbFetch(check5)

check6 <- dbSendQuery(con, "
select sum(overpayment) , sum(num_customers)
from final_products.rate_comparison
where edc = 'UI'
and contractterm > 36                      
                      ")
res_ch6 <- dbFetch(check6)

res_ch1
res_ch2
res_ch3
res_ch4
res_ch5
res_ch6

