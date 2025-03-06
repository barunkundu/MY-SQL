


use data_cleaning;
select * from layoffs;

create table barun like layoffs;
insert barun select * from layoffs;

select * from barun;

-- ----------------------------------  ind duplicate using window key -------------------


select *, -- set unique number- 1
row_number() 
over(partition by company, location, industry, total_laid_off, stage, country, funds_raised_millions, `date`) as uni_num
from barun;

-- find duplicates
with duplicates as(
	select *,
	row_number() 
    over(partition by company, location, industry, total_laid_off, stage, country, funds_raised_millions, `date`) as uni_num
	from barun
)
select * from duplicates
where uni_num > 1;

select * from barun
where company = "Casper";


-- delete duplicate
/*with duplicates as(
	select *,
	row_number() 
    over(partition by company, location, industry, total_laid_off,
    stage, country, funds_raised_millions, `date`) as uni_num
	from barun
)
delete from duplicates
where uni_num > 1; -- don't working*/


-- make a table using barun then delete the duplicates


CREATE TABLE `barun1` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `uni_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into barun1 
select *, -- set unique number- 1
row_number() 
over(partition by company, location, industry, total_laid_off, stage, country, funds_raised_millions, `date`) as uni_num
from barun;
;


-- now delete the duplicates
delete from barun1
where uni_num >1;

select * from barun1;



-- --------------------------------------------   standarizing data    ------------------



-- standarizing data
-- trim all 

-- company
/*update barun1 
set company = trim(company);*/

select company, trim(company)
from barun1; -- show befor after condition

select distinct company
from barun1;

-- industry
update barun1 
set industry = trim(industry);

select distinct industry -- distince deyar karon null & faka gula age dekhabe
from barun1
order by 1;

select * from barun1
where industry like "crypto%"; -- show kore j tar samone cripto ache 

-- change industry cripto
update barun1
set industry = 'crypto'
where industry like "crypto%";


-- country 
select distinct country
from barun1
order by 1;

select * from barun1 
where country = 'United States.';

-- change this
update barun1 
set country = 'United States'
where country like 'United States%';

select country, trim(country)
from barun1;

-- for advance trim and change at a time
/*select country, trim(trailing '.' from company)
from barun1
where country like 'United States%';*/


select * from barun1;

-- stage
update barun1
set stage = trim(stage);


-- location
select distinct location
from barun1
order by 1;

update barun1
set location = trim(location);

-- test to date formate 
select `date` from barun1;

--  using a popular formate this
select `date`, 
str_to_date(`date`, '%m/%d/%Y') as date_1
from barun1; -- check 1st 

update barun1
set `date` = str_to_date(`date`, '%m/%d/%Y');

-- now modify this text to date 
alter table barun1
modify `date` date;



-- ------------------------------------- 3. handeling with null value ---------------------------------

select total_laid_off from barun1
where total_laid_off is NULL 
or
total_laid_off = '';


select * from barun1
where percentage_laid_off is NULL 
or
percentage_laid_off = '';

select * from barun1
where industry is NULL 
or
industry = '';

-- do blank to null

select * from barun1 
where company = "Airbnb";

-- use self join to find industrys
select t1.industry, t2.industry from 
barun1 as t1
join barun1 as t2
	on t1.company = t2.company
 where t1.industry is NULL 
 and t2.industry is not NULL;
 
update
barun1 as t1
join barun1 as t2
	on t1.company = t2.company
    set t1.industry =  t2.industry
 where t1.industry is NULL 
 and t2.industry is not NULL;

select * from barun1
where total_laid_off is NULL
and percentage_laid_off is null;

delete from barun1
where total_laid_off is NULL
and percentage_laid_off is null;

alter table barun1
drop column uni_num;

select * from barun1;
select count(*) as total_r from barun1;





-- ------------------------------------------- data analysis -----------------------------------------------------

select percentage_laid_off from barun1
where percentage_laid_off is null;

select company, sum(total_laid_off)
from barun1
group by company
order by 2 desc;

select * from barun1
where `date` is NULL;

select industry, sum(total_laid_off)
from barun1
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from barun1
group by country
order by 2 desc;

select `date`, sum(total_laid_off)
from barun1
group by `date`
order by 2 desc;

select year(`date`), sum(total_laid_off)
from barun1
where year(`date`) is not null
group by year(`date`)
order by 2 desc;

select substring(`date`, 1,7) as `month`, sum(total_laid_off)
from barun1
where substring(`date`, 1, 7) is not null
group by `month`
order by 1 asc;


with rolling_sum as
(
select substring(`date`, 1,7) as `month`, sum(total_laid_off)as total_laid
from barun1
where substring(`date`, 1, 7) is not null
group by `month`
order by 1 asc
)
select  `month`, total_laid,
sum(total_laid) over(order by `month`) as roll_sum_laid
from rolling_sum;

with company_rank (c_n, years, total_laid) as 
(
select company, year(`date`) , sum(total_laid_off) as t1
from barun1
group by company, year(`date`)
order by 3 desc
)
select * ,
dense_rank() over(partition by years order by total_laid desc) as rank1
from company_rank
where years is not null
order by rank1 asc;


-- now find top 5 rank holder

with company_year (company_name, years, total_laid) as 
(
select company, year(`date`) , sum(total_laid_off) as t1
from barun1
group by company, year(`date`)

), company_year_rank as (
select * ,
dense_rank() over(partition by years order by total_laid desc) as rank1
from company_year
where years is not null
)
select * -- find top 4 company based on their t_l_o based on year
from company_year_rank
where rank1 <5; 


select * from barun1;





-- -----------------      based on industry     ---------------------------


select year(`date`) as years, sum(percentage_laid_off)
from barun1
where year(`date`) is not null
group by years;

select industry, year(`date`) as years, sum(percentage_laid_off)
from barun1
where year(`date`) is not null
group by industry, years
order by 3 desc;

with industry_rank (industry, years, percentage_laid_off) as 
(
select industry, year(`date`) as years, round(sum(percentage_laid_off),2)
from barun1
where year(`date`) is not null
group by industry, years
order by 3 desc
),
temp as
(
select *,
dense_rank() over(partition by years order by percentage_laid_off desc) as rank1
from industry_rank
)
select * from temp
where rank1 <2;
;






with industry_rank (industry, years, p_l_off) as 
(
select industry, year(`date`) as years, sum(percentage_laid_off)
from barun1
where year(`date`) is not null
group by industry, years
order by 3 desc
)
select *,
rank() over(partition by years order by p_l_off desc) as rank1
from industry_rank;

select * from barun1;