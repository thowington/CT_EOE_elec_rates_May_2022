
drop table if exists q6.all; 
create table q6.all as select * from q6.ambit;

insert into q6.all select * from q6.atlantic;
insert into q6.all select * from q6.clearview; 
insert into q6.all select * from q6.cne;
insert into q6.all select * from q6.ctge;
insert into q6.all select * from q6.direct;
insert into q6.all select * from q6.energyplus;
insert into q6.all select * from q6.er;
insert into q6.all select * from q6.major;
insert into q6.all select * from q6.mega;
insert into q6.all select * from q6.nap;
insert into q6.all select * from q6.nge;
insert into q6.all select * from q6.public;
insert into q6.all select * from q6.reliant;
insert into q6.all select * from q6.residents;
insert into q6.all select * from q6.spark;
insert into q6.all select * from q6.starion;
insert into q6.all select * from q6.think;
insert into q6.all select * from q6.townsquare;
insert into q6.all select * from q6.verde;
insert into q6.all select * from q6.viridian;
insert into q6.all select * from q6.wattifi;
insert into q6.all select * from q6.xoom;

-- cleanup
delete from q6.all
where billed_rate is NULL
or totalkwh is NULL

delete from q6.all
where totalkwh < 0

delete from q6.all
where billed_rate <0

-- remove non-CT zips
delete from q6.all
where zipcode not like '06%'

delete from q6.all
where length(zipcode) !=5


-- make edc names uniform
select distinct(edc) from q6.all;

update q6.all
set edc = 'UI'
where edc in ('UIC','United Illum','United Illuminating','UI');

update q6.all
set edc = 'EV'
where edc in ('Eversource (CL&P)','Eversource Energy - CT',
			  'Connecticut Light & Power','Connecticut Light and Power','Eversource',
			 'CL&P','CLP');

select distinct(edc) from q6.all;

---

select supplier, count(supplier) as rec_count,
	sum(billed_rate) as billed_rate,
	sum(num_customers) as num_customers,
	sum(totalkwh) as total_kwh,
	sum(contractterm) as contract_term
	from q6.all
	group by supplier