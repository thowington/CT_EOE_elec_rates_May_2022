# participation rate by ZCTA ----


library(RPostgres)
library(odbc)
library(dplyr)
library(readxl)
library(ConfigParser)
library(stringr)
library(ggplot2)
library(reshape2)

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

#  no longer using households from ACS as denominator.  Instead using data from EDCs.
# # get participants by zipcode for Jan 2020 ----
# res <- dbSendQuery(con, "select zipcode,
#             sum(num_customers) as participating_customers
#             from final_products.rate_comparison
#             where year_charge = '2020'
#             and month_charge = '01'
#             group by zipcode")
# num_participating_households <- dbFetch(res) %>%
#   mutate(zipcode = str_trim(zipcode, side = c("both")))
# 
# # join to ZCTA and summarize ----
# res2 <- dbSendQuery(con, "select * from acs.crosswalk")
# crosswalk <- dbFetch(res2)
# 
# num_participating_households_by_zcta <- num_participating_households %>%
#   left_join(crosswalk, by.x = zipcode,by.y = zipcode)
# 
# # join to total households by ZCTA ----
# res3 <- dbSendQuery(con,"select zcta, count_households, median_hh_income from acs.income")
# total_households <- dbFetch(res3)
# partcipation_rate_zcta_jan_2020 <- num_participating_households_by_zcta %>%
#   left_join(total_households, by.x = zcta, by.y = zcta) %>%
#   rename(total_hhs = count_households) %>%
#   mutate(participation_rate = participating_customers / total_hhs) %>%
#   select(zcta, participating_customers, total_hhs, median_hh_income, participation_rate) %>%
#   na.omit()
# 
# 
# dbWriteTable(con, name = Id(schema = 'final_products', table = 'participation_rate_jan_2020'), 
#              value = partcipation_rate_zcta_jan_2020, overwrite = TRUE, row.names = FALSE)


# # make plot of participation rate versus median_hh_income ----
# plotdata=partcipation_rate_zcta_jan_2020 %>% filter(participation_rate < 1.0)
# regr_res <- lm(participation_rate ~ median_hh_income, data = plotdata)
# theme_set(theme_gray(base_size = 50))
# filename = paste0("C:/Users/thowi/Documents/consulting_work/CT_EOE_elec_rates_May_2022/output/participation_rate_Jan_2020.png")
# png(filename=filename,
#     width=1.35*1250,
#     height=1*1250)
# ggplot(data=plotdata,aes(x=median_hh_income,y=participation_rate))+
#   geom_point(color = "red", size = 3) +
#   #geom_abline(intercept = regr_res$coefficients[1],slope = regr_res$coefficients[2]) +
#   labs(x="Median Household Income",y="Participation Rate by ZCTA")+
#   theme(legend.position = c(.89,.2),
#         legend.margin=margin(2,1,1,1,"line"),
#         legend.key.height=unit(4,"line"))
# dev.off()


# calc participation rate from EDC-supplied data ----
infile = paste0(project_dir, "/Eversource_responses/PURA_EOE_11_EOE_038_1_Attachment A.xlsx")
ev_resp <- read_excel(infile, sheet = "Attachment A")
ev_resp <- ev_resp %>% rename(zip = `Zip Code`,
                              year = `Year End`,
                              num_custs_supplier = `Supplier Customer Count`,
                              num_custs_edc = `EDC Customer Count`,
                              tot_custs = `Total Customer Count`) %>%
  mutate(edc = "Eversource")
ev_resp[is.na(ev_resp)] <-0
ev_resp

ev_sums_2021 <- ev_resp %>% filter(year == '2021') %>% 
  summarise(tot_sup = sum(num_custs_supplier),
            tot_edc = sum(num_custs_edc),
            total = sum(tot_custs))



infile = paste0(project_dir, "/UI_responses/EOE-038 UI Attachment 1 Revised.xlsx")
ui_resp <- read_excel(infile, sheet = "Sheet1")
ui_resp <- ui_resp %>% rename(zip = `Zip Code`,
                              year = `Year-end`,
                              num_custs_supplier = `Supplier \r\nCustomer Count`,
                              num_custs_edc = `EDC \r\nCustomer Count`,
                              tot_custs = `Total \r\nCustomer Count`) %>%
  mutate(edc = "UI")
ui_resp[is.na(ui_resp)] <-0
ui_resp

ui_sums_2021 <- ui_resp %>% filter(year == '2021') %>% 
  summarise(tot_sup = sum(num_custs_supplier),
            tot_edc = sum(num_custs_edc),
            total = sum(tot_custs))


# load edc-supplied data into database ----
dbSendQuery(con, "drop schema if exists q38 cascade")
dbSendQuery(con, "create schema q38")
dbSendQuery(con, "drop table if exists q38.participation_data")
dbSendQuery(con, "create table q38.participation_data (
            zip char(5),
            year char(4),
            num_custs_supplier integer,
            num_custs_edc integer,
            tot_custs integer,
            edc char(10)
)")

dbWriteTable(con, name = Id(schema = 'q38', table = 'participation_data'), value = ev_resp, append = TRUE, row.names = FALSE)
dbWriteTable(con, name = Id(schema = 'q38', table = 'participation_data'), value = ui_resp, append = TRUE, row.names = FALSE)

# aggregate by year and zip ----
dbSendQuery(con, "drop table if exists q38.part_data_aggregated")
dbSendQuery(con, "create table q38.part_data_aggregated (
            zip char(5),
            year char(4),
            supplier_customers integer,
            edc_customers integer,
            total_customers integer
)")
dbSendQuery(con, "insert into q38.part_data_aggregated
  select zip, 
  year, 
  sum(num_custs_supplier) as supplier_customers,
  sum(num_custs_edc) as edc_customers,
  sum(tot_custs) as total_customers
  from q38.participation_data
  group by zip, year")

dbSendQuery(con, "drop table if exists q38.part_data_zcta")
dbSendQuery(con, "  select a.*,
  c.zcta,
  c.median_hh_income 
into q38.part_data_zcta
from q38.part_data_aggregated a
join acs.crosswalk b
on a.zip = b.zipcode
join acs.income c
on b.zcta = c.zcta")

dbSendQuery(con, "drop table if exists final_products.participation_rate_by_year_zcta")
dbSendQuery(con, "  select year,
  zcta,
  sum(supplier_customers) as supplier_customers,
  sum(edc_customers) as edc_customers,
  sum(total_customers) as total_customers,
  sum(supplier_customers) *1.0 / sum(total_customers) as participation_rate,
  median_hh_income
into final_products.participation_rate_by_year_zcta
from q38.part_data_zcta
group by year, zcta, median_hh_income")
  
# participation rates by year ----
res2017 <- dbSendQuery(con, 
                    "select * from final_products.participation_rate_by_year_zcta
                    where year = '2017'")
part_rate_2017 <- dbFetch(res2017)


res2018 <- dbSendQuery(con, 
                    "select * from final_products.participation_rate_by_year_zcta
                    where year = '2018'")
part_rate_2018 <- dbFetch(res2018)

res2019 <- dbSendQuery(con, 
                       "select * from final_products.participation_rate_by_year_zcta
                    where year = '2019'")
part_rate_2019 <- dbFetch(res2019)

res2020 <- dbSendQuery(con, 
                       "select * from final_products.participation_rate_by_year_zcta
                    where year = '2020'")
part_rate_2020 <- dbFetch(res2020)

res2021 <- dbSendQuery(con, 
                       "select * from final_products.participation_rate_by_year_zcta
                    where year = '2021'")
part_rate_2021 <- dbFetch(res2021)


# make plot 2017 ----
plotdata=part_rate_2017
#regr_res <- lm(participation_rate ~ median_hh_income, data = plotdata)
theme_set(theme_gray(base_size = 50))
filename = paste0(project_dir,"/output/participation_rate_edc_data_2017.png")
png(filename=filename,
    width=1.35*1500,
    height=1*1250)
ggplot(data=plotdata,aes(x=median_hh_income,y=participation_rate))+
  geom_point(color = "red", size = 3) +
  ggtitle("Participation Rate vs Median Houshold Income, 2017")+
#  geom_abline(intercept = regr_res$coefficients[1],slope = regr_res$coefficients[2]) +
  labs(x="Median Household Income",y="Participation Rate by ZCTA")+
  theme(legend.position = c(.89,.2),
        legend.margin=margin(2,1,1,1,"line"),
        legend.key.height=unit(4,"line"),
        plot.title = element_text(size = 20))
dev.off()

# make plot 2018 ----
plotdata=part_rate_2018
#regr_res <- lm(participation_rate ~ median_hh_income, data = plotdata)
theme_set(theme_gray(base_size = 50))
filename = paste0(project_dir,"/output/participation_rate_edc_data_2018.png")
png(filename=filename,
    width=1.35*1500,
    height=1*1250)
ggplot(data=plotdata,aes(x=median_hh_income,y=participation_rate))+
  geom_point(color = "red", size = 3) +
  ggtitle("Participation Rate vs Median Houshold Income, 2018")+
#  geom_abline(intercept = regr_res$coefficients[1],slope = regr_res$coefficients[2]) +
  labs(x="Median Household Income",y="Participation Rate by ZCTA")+
  theme(legend.position = c(.89,.2),
        legend.margin=margin(2,1,1,1,"line"),
        legend.key.height=unit(4,"line"),
        plot.title = element_text(size = 20))
dev.off()

# make plot 2019 ----
plotdata=part_rate_2019
#regr_res <- lm(participation_rate ~ median_hh_income, data = plotdata)
theme_set(theme_gray(base_size = 50))
filename = paste0(project_dir,"/output/participation_rate_edc_data_2019.png")
png(filename=filename,
    width=1.35*1500,
    height=1*1250)
ggplot(data=plotdata,aes(x=median_hh_income,y=participation_rate))+
  geom_point(color = "red", size = 3) +
  ggtitle("Participation Rate vs Median Houshold Income, 2019")+
#  geom_abline(intercept = regr_res$coefficients[1],slope = regr_res$coefficients[2]) +
  labs(x="Median Household Income",y="Participation Rate by ZCTA")+
  theme(legend.position = c(.89,.2),
        legend.margin=margin(2,1,1,1,"line"),
        legend.key.height=unit(4,"line"),
        plot.title = element_text(size = 20))
dev.off()

# make plot 2020 ----
plotdata=part_rate_2020
#regr_res <- lm(participation_rate ~ median_hh_income, data = plotdata)
theme_set(theme_gray(base_size = 50))
filename = paste0(project_dir,"/output/participation_rate_edc_data_2020.png")
png(filename=filename,
    width=1.35*1500,
    height=1*1250)
ggplot(data=plotdata,aes(x=median_hh_income,y=participation_rate))+
  geom_point(color = "red", size = 3) +
  ggtitle("Participation Rate vs Median Houshold Income, 2020")+
#geom_abline(intercept = regr_res$coefficients[1],slope = regr_res$coefficients[2]) +
  labs(x="Median Household Income",y="Participation Rate by ZCTA")+
  theme(legend.position = c(.89,.2),
        legend.margin=margin(2,1,1,1,"line"),
        legend.key.height=unit(4,"line"),
        plot.title = element_text(size = 20))
dev.off()


# make plot 2021 ----
plotdata=part_rate_2021
#regr_res <- lm(participation_rate ~ median_hh_income, data = plotdata)
theme_set(theme_gray(base_size = 50))
filename = paste0(project_dir,"/output/participation_rate_edc_data_2021.png")
png(filename=filename,
    width=1.35*1500,
    height=1*1250)
ggplot(data=plotdata,aes(x=median_hh_income,y=participation_rate))+
  geom_point(color = "red", size = 3) +
  ggtitle("Participation Rate vs Median Houshold Income, 2021")+
#  geom_abline(intercept = regr_res$coefficients[1],slope = regr_res$coefficients[2]) +
  labs(x="Median Household Income",y="Participation Rate by ZCTA")+
  theme(legend.position = c(.89,.2),
        legend.margin=margin(2,1,1,1,"line"),
        legend.key.height=unit(4,"line"),
        plot.title = element_text(size = 20))
dev.off()

filename = paste0(project_dir, "/output/part_rate_2017.csv")
write.csv(part_rate_2017, filename, row.names = FALSE)
filename = paste0(project_dir, "/output/part_rate_2018.csv")
write.csv(part_rate_2018, filename, row.names = FALSE)
filename = paste0(project_dir, "/output/part_rate_2019.csv")
write.csv(part_rate_2019, filename, row.names = FALSE)
filename = paste0(project_dir, "/output/part_rate_2020.csv")
write.csv(part_rate_2020, filename, row.names = FALSE)
filename = paste0(project_dir, "/output/part_rate_2021.csv")
write.csv(part_rate_2021, filename, row.names = FALSE)

# make plots with income groupings ----
res2017_grouped <- dbSendQuery(con,
"with grouped_data as (select *,
median_hh_income/50000 as income_group
from final_products.participation_rate_by_year_zcta
where year = '2017' and median_hh_income is not NULL
)
select year,income_group,
	sum(supplier_customers) as supplier_customers,
	sum(edc_customers) as edc_customers,
	sum(total_customers) as total_customers,
	sum(supplier_customers)*1.0/sum(total_customers) as participation_rate
from grouped_data
group by year, income_group
order by income_group")
part_rate_2017_grouped <- dbFetch(res2017_grouped)
part_rate_2017_grouped
part_rate_2017_grouped <- part_rate_2017_grouped %>%
  mutate(income_group = case_when(income_group == 0 ~ "Less than $50,000",
                                  income_group == 1 ~ "$50,000 - $100,000",
                                  income_group == 2 ~ "$100,000 - $150,000",
                                  income_group == 3 ~ "$150,000 - $200,000",
                                  income_group == 4 ~ "More than $200,000"))
part_rate_2017_grouped
filename = paste0(project_dir, "/output/participation_rate_grouped_2017.csv")
write.csv(part_rate_2017_grouped, filename, row.names = FALSE)



res2018_grouped <- dbSendQuery(con,
                               "with grouped_data as (select *,
median_hh_income/50000 as income_group
from final_products.participation_rate_by_year_zcta
where year = '2018' and median_hh_income is not NULL
)
select year, income_group,
	sum(supplier_customers) as supplier_customers,
	sum(edc_customers) as edc_customers,
	sum(total_customers) as total_customers,
	sum(supplier_customers)*1.0/sum(total_customers) as participation_rate
from grouped_data
group by year, income_group
order by income_group")
part_rate_2018_grouped <- dbFetch(res2018_grouped)
part_rate_2018_grouped
part_rate_2018_grouped <- part_rate_2018_grouped %>%
  mutate(income_group = case_when(income_group == 0 ~ "Less than $50,000",
                                  income_group == 1 ~ "$50,000 - $100,000",
                                  income_group == 2 ~ "$100,000 - $150,000",
                                  income_group == 3 ~ "$150,000 - $200,000",
                                  income_group == 4 ~ "More than $200,000"))
part_rate_2018_grouped
filename = paste0(project_dir, "/output/participation_rate_grouped_2018.csv")
write.csv(part_rate_2018_grouped, filename, row.names = FALSE)






res2019_grouped <- dbSendQuery(con,
                               "with grouped_data as (select *,
median_hh_income/50000 as income_group
from final_products.participation_rate_by_year_zcta
where year = '2019' and median_hh_income is not NULL
)
select year, income_group,
	sum(supplier_customers) as supplier_customers,
	sum(edc_customers) as edc_customers,
	sum(total_customers) as total_customers,
	sum(supplier_customers)*1.0/sum(total_customers) as participation_rate
from grouped_data
group by year, income_group
order by income_group")
part_rate_2019_grouped <- dbFetch(res2019_grouped)
part_rate_2019_grouped
part_rate_2019_grouped <- part_rate_2019_grouped %>%
  mutate(income_group = case_when(income_group == 0 ~ "Less than $50,000",
                                  income_group == 1 ~ "$50,000 - $100,000",
                                  income_group == 2 ~ "$100,000 - $150,000",
                                  income_group == 3 ~ "$150,000 - $200,000",
                                  income_group == 4 ~ "More than $200,000"))
part_rate_2019_grouped
filename = paste0(project_dir, "/output/participation_rate_grouped_2019.csv")
write.csv(part_rate_2019_grouped, filename, row.names = FALSE)





res2020_grouped <- dbSendQuery(con,
                               "with grouped_data as (select *,
median_hh_income/50000 as income_group
from final_products.participation_rate_by_year_zcta
where year = '2020' and median_hh_income is not NULL
)
select year, income_group,
	sum(supplier_customers) as supplier_customers,
	sum(edc_customers) as edc_customers,
	sum(total_customers) as total_customers,
	sum(supplier_customers)*1.0/sum(total_customers) as participation_rate
from grouped_data
group by year, income_group
order by income_group")
part_rate_2020_grouped <- dbFetch(res2020_grouped)
part_rate_2020_grouped
part_rate_2020_grouped <- part_rate_2020_grouped %>%
  mutate(income_group = case_when(income_group == 0 ~ "Less than $50,000",
                                  income_group == 1 ~ "$50,000 - $100,000",
                                  income_group == 2 ~ "$100,000 - $150,000",
                                  income_group == 3 ~ "$150,000 - $200,000",
                                  income_group == 4 ~ "More than $200,000"))
part_rate_2020_grouped
filename = paste0(project_dir, "/output/participation_rate_grouped_2020.csv")
write.csv(part_rate_2020_grouped, filename, row.names = FALSE)



# make grouped plot 2021 ----
res2021_grouped <- dbSendQuery(con,
                               "with grouped_data as (select *,
median_hh_income/50000 as income_group
from final_products.participation_rate_by_year_zcta
where year = '2021' and median_hh_income is not NULL
)
select year, income_group,
	sum(supplier_customers) as supplier_customers,
	sum(edc_customers) as edc_customers,
	sum(total_customers) as total_customers,
	sum(supplier_customers)*1.0/sum(total_customers) as participation_rate
from grouped_data
group by year, income_group
order by income_group")
part_rate_2021_grouped <- dbFetch(res2021_grouped)
part_rate_2021_grouped<- part_rate_2021_grouped %>%
  mutate(income_group = case_when(income_group == 0 ~ "Less than $50,000",
                                  income_group == 1 ~ "$50,000 - $100,000",
                                  income_group == 2 ~ "$100,000 - $150,000",
                                  income_group == 3 ~ "$150,000 - $200,000",
                                  income_group == 4 ~ "More than $200,000"))
part_rate_2021_grouped
filename = paste0(project_dir, "/output/participation_rate_grouped_2021.csv")
write.csv(part_rate_2021_grouped, filename, row.names = FALSE)


#  aggregate participation by years and income bin ----
part_rate_income_years <- rbind(part_rate_2017_grouped,
                                part_rate_2018_grouped,
                                part_rate_2019_grouped,
                                part_rate_2020_grouped,
                                part_rate_2021_grouped)
part_rate_income_years_pivot <- dcast(part_rate_income_years, income_group ~ year, mean) %>%
  rename("Median Household Income" = income_group) %>%
  select("Median Household Income","2017","2018","2019","2020", "2021") 
part_rate_income_years_pivot


filename = paste0(project_dir, "/output/part_rate_income_years_pivot.csv")
write.csv(part_rate_income_years_pivot, filename, row.names = FALSE)





# plotdata=part_rate_2021_grouped
# theme_set(theme_gray(base_size = 50))
# filename = paste0(project_dir,"/output/participation_rate_grouped_income_2021.png")
# png(filename=filename,
#     width=1.35*1250,
#     height=1*1250)
# ggplot(data=plotdata,aes(x=income_group,y=participation_rate))+
#   geom_col(color = "red", size = 3) +
#   labs(x="Median Household Income",y="Participation Rate by ZCTA")+
#   theme(legend.position = c(.89,.2),
#         legend.margin=margin(2,1,1,1,"line"),
#         legend.key.height=unit(4,"line"))
# dev.off()

#  grouping by gt CT median and lt CT median ----
# 2021 median household income in CT was $80,958
res2017_gtlt_median <- dbSendQuery(con,
  "select sum(supplier_customers) as supplier_customers,
	sum(edc_customers) as edc_customers,
	sum(total_customers) as total_customers,
	sum(supplier_customers)*1.0/sum(total_customers) as participation_rate,
	'Less than CT Median Household Income' as income_group
from final_products.participation_rate_by_year_zcta
where year = '2017' and median_hh_income < 80958
union
select sum(supplier_customers) as supplier_customers,
	sum(edc_customers) as edc_customers,
	sum(total_customers) as total_customers,
	sum(supplier_customers)*1.0/sum(total_customers) as participation_rate,
	'Greater than CT Median Household Income' as income_group
from final_products.participation_rate_by_year_zcta
where year = '2017' and median_hh_income >= 80958
")
part_rate_2017_gtlt_median <- dbFetch(res2017_gtlt_median)
part_rate_2017_gtlt_median

filename = paste0(project_dir, "/output/part_rate_2017_gtlt_median.csv")
write.csv(part_rate_2017_gtlt_median, filename, row.names = FALSE)


# make median income plot 2021 ----
res2021_gtlt_median<- dbSendQuery(con,
  "select sum(supplier_customers) as supplier_customers,
	sum(edc_customers) as edc_customers,
	sum(total_customers) as total_customers,
	sum(supplier_customers)*1.0/sum(total_customers) as participation_rate,
	'Less than CT Median Household Income' as income_group
from final_products.participation_rate_by_year_zcta
where year = '2021' and median_hh_income < 80958
union
select sum(supplier_customers) as supplier_customers,
	sum(edc_customers) as edc_customers,
	sum(total_customers) as total_customers,
	sum(supplier_customers)*1.0/sum(total_customers) as participation_rate,
	'Greater than CT Median Household Income' as income_group
from final_products.participation_rate_by_year_zcta
where year = '2021' and median_hh_income >= 80958
")
part_rate_2021_gtlt_median <- dbFetch(res2021_gtlt_median)
part_rate_2021_gtlt_median

filename = paste0(project_dir, "/output/part_rate_2021_gtlt_median.csv")
write.csv(part_rate_2021_gtlt_median, filename, row.names = FALSE)


# make table num_sups, num_invoices, part_rate ----
part_rate_years <- dbSendQuery(con, "with sups as (select year_charge, 
count(distinct(supplier)) as num_suppliers, 
sum(num_customers) as num_invoices,
sum(num_customers)*1.0/12 as avg_num_hhlds
from q6.all 
group by year_charge
order by year_charge ),
parts as (
select year, sum(supplier_customers)*1.0/sum(total_customers) as part_rate
from q38.part_data_aggregated
group by year
order by year)
select year_charge, num_suppliers, num_invoices, avg_num_hhlds, part_rate
from sups a
join parts b
on a.year_charge = b.year")

# participation rate by year ----
part_rate_years_table <- dbFetch(part_rate_years)
part_rate_years_table

filename = paste0(project_dir, "/output/part_rate_years_table.csv")
write.csv(part_rate_years_table, filename, row.names = FALSE)

# participation rate by EDC and year ----
res_part_rate_by_EDC_year <- dbSendQuery(con, 
                       "with EV_pr as (
                      select year, sum(num_custs_supplier) *1.0 / sum(tot_custs) as Eversource_part_rate
                      from q38.participation_data
                      where edc = 'Eversource'
                      group by year
                      order by year),
                      UI_pr as (select year, sum(num_custs_supplier) *1.0 / sum(tot_custs) as UI_part_rate
                      from q38.participation_data
                      where edc = 'UI'
                      group by year
                      order by year)
                      select a.year, a.Eversource_part_rate, b.UI_part_rate
                      from EV_pr a
                      join UI_pr b
                      on a.year = b.year")
part_rate_by_EDC_year <- dbFetch(res_part_rate_by_EDC_year)

filename = paste0(project_dir, "/output/part_rate_EDC_year.csv")
write.csv(part_rate_by_EDC_year, filename, row.names = FALSE)
dbWriteTable(con, name = Id(schema = 'q38', table = 'participation_by_edc_year'), value = part_rate_by_EDC_year, overwrite = TRUE, row.names = FALSE)

# zctas with participation rate greater than 75% ----
# n.b.: ZIP 06020 is just a post office - no households there
res_part_rate_gt_75_pct <- dbSendQuery(con, "select distinct a.zcta, b.po_name, c.median_hh_income, c.count_households, a.participation_rate
  from final_products.participation_rate_by_year_zcta a
  join acs.crosswalk b
  on a.zcta = b.zcta
  join acs.income c
  on a.zcta = c.zcta
  where a.year = '2021'
  and a.participation_rate > .75
  and a.zcta != '06020'
  order by a.participation_rate desc")
part_rate_gt_75_pct <- dbFetch(res_part_rate_gt_75_pct)
part_rate_gt_75_pct
filename = paste0(project_dir, "/output/part_rate_gt_75_pct.csv")
write.csv(part_rate_gt_75_pct, filename, row.names = FALSE)


print("finished participation rate work.")