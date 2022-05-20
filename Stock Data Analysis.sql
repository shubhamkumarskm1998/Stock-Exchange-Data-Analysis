drop table stock_companies;
CREATE EXTERNAL TABLE IF NOT EXISTS STOCK_PRICES(
    Trading_date Date,
    Symbol String,
    Open double,
    Close double,
    Low double,
    High double,
    Volume inT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
TBLPROPERTIES("skip.header.line.count"="1");
load data inpath '/user/anabig114215/StockPrices.csv' INTO TABLE STOCK_PRICES;

CREATE EXTERNAL TABLE IF NOT EXISTS STOCK_COMPANIES(
    Symbol String,
    Company_name String,
    Sector String,
    Sub_industry String,
    Headquarter String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
TBLPROPERTIES("skip.header.line.count"="1");
load data inpath '/user/anabig114215/Stockcompanies.csv' INTO TABLE STOCK_COMPANIES;

SELECT * FROM stock_companies LIMIT 5;
SELECT * FROM stock_prices LIMIT 5;
drop table stock;
CREATE TABLE STOCK as 
SELECT  year(to_date(trading_date)) as s_year,
        month(to_date(trading_date)) as s_month,
        c.symbol,
        Company_name,
        replace(regexp_extract(Headquarter,";.*",0),";","") as State,
        sector,
        sub_industry,
        round(avg(open),2) as open, 
        round(avg(close),2) as close, 
        round(avg(low),2) as low,
        round(avg(high),2) as high,
        round(avg(volume),2) as volume 
from stock_prices as p 
inner join stock_companies as c on p.symbol=c.symbol 
group by month(to_date(trading_date)), year(to_date(trading_date)), Company_name,
        replace(regexp_extract(Headquarter,";.*",0),";",""),sector,
        sub_industry,c.symbol;
    
select * from stock limit 5;

---------------------------------------
--Q1.) Find the top five companies that are good for investment
select company_name,min(s_year) as starting_year, max(s_year) as current_year ,min(open) as  open_price ,max(close)  as current_price , round(((max(close)-min(open))*100)/min(open),2) as percentage_growth,
round(((max(close)-min(open))*100)/min(open)/6,2) as percentage_by_year
from stock
group by company_name
order by percentage_by_year desc limit 5

--Q2.) Show the best-growing industry by each state, having at least two or more industries mapped.
select state,sector,count(distinct sub_industry) as mapped_industries_count, round(((max(close)-min(open))*100)/min(open)/6,2) as percentage_by_year
from stock
group by state,sector
having count(distinct sub_industry) >2
order by percentage_by_year desc limit 5

--Q3.)5) For each sector find the following.
--a. Worst year b. Best year c. Stable year


create table Tab as select sector,s_year,avg(close) as b,lead(avg(close),1) over (partition by sector order by s_year) as a from stock
     group by sector,s_year 
     order by sector,s_year
create Table tab2 as select sector,s_year, (case when a is null then null else
 round((a-b),2) end) as cummulative_change_yearly from Tab
    
select sector, (select sector, s_year from tab2 where  (case when a is null then null else
 round((a-b),2) end) <'0' order by  cummulative_change_yearly asc limit 1) as worst_year from tab2

create table tab3 as 
select sector, s_year,cummulative_change_yearly,ROW_NUMBER() OVER(PARTITION BY sector order by cummulative_change_yearly  ) as rnk from tab2
where cummulative_change_yearly is not null

select sector ,s_year as worst_year from tab3 
where rnk=1) 
select sector ,s_year as best_year from tab3 
where rnk=6
select sector ,s_year as stable_year from tab3 
where rnk=3