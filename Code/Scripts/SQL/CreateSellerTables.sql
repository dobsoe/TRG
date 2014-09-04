/*===========================================================================
* TRG Fashion Analytics Lab 600
*
* Author: Ellie Dobson
* Date:   2014-09-02 
*
* Copyright 2014, Pivotal
*
* DESCRIPTION:
* This script creates the table on which the sellers filters should be run to define best and worst sellers
*
* NOTE: The hard coded date at the end should be replaced for today() when the script is put into production
*/

---------------------------------------------
-- Performs aggregation over date for each item in all_master_clean
-- Item defined as a distinct json_productkey+subcategory+size+colour
-- Extracts the latest batch date, whether product has been out of stock, whether there have been product refreshes and the date of first markdown over the date range
---------------------------------------------

drop view if exists ds.tmp_agg_date cascade;
create view ds.tmp_agg_date as 
       select 
          p.json_productkey, 
	  p.size,
	  p.subcategory, 
	  p.taxonomiccolour,
	  -- latest batch date recorded for each item
          max(p.batch_date) as batch_date,
          -- whether item has been out of stock at least once
          max(p.outofstock) as outofstock_atleastonce,
          -- whether there have been any product refreshes
          max(case when p.outofstock_nextday = 0 and p.outofstock = 1 then 1 else 0 end) as wasrefreshed_atleastonce,
          -- date where product was first marked down (left blank if product never marked down)
          b.firstmarkdowndate as firstmarkdown_date
          
   from (select json_productkey, size, subcategory, taxonomiccolour, batch_date, outofstock, lead(outofstock,1) over (partition by json_productkey, size order by batch_date) as outofstock_nextday from ds.all_master_clean) 
   as p

   left outer join 
   (
	select
       	  json_productkey, 
	  size,
	  subcategory,
	  taxonomiccolour,
       	  min(batch_date) as firstmarkdowndate
   	from ds.all_master_clean
   	where markdown!=0
   	group by json_productkey, size, subcategory, taxonomiccolour
   ) 
   as b
   on (b.json_productkey=p.json_productkey) and (b.size=p.size) and (b.taxonomiccolour=p.taxonomiccolour)
   group by p.json_productkey, p.subcategory, p.size, p.taxonomiccolour, firstmarkdown_date	
;

---------------------------------------------
-- Joins tmp_agg_date with the other data from all_master_clean
-- Join using json_productkey, size, batch_date, subcategory and taxonomiccolour
---------------------------------------------

drop view if exists ds.all_agg_date cascade;
create view ds.all_agg_date as 
       select
	       a.json_productkey, 
	       b.batch_date as latest_batch_date, 
	       b.outofstock_atleastonce, 
	       b.firstmarkdown_date, 
	       b.wasrefreshed_atleastonce,
	       a.country,
	       a.category, 
    	       a.subcategory,
    	       a.retailername, 
	       a.facetbrand,
	       a.size,
	       a.iscoresize,
	       a.iscoreitem,	       	      
	       a.taxonomiccolour,
	       a.newin_7, 
	       a.newin_30,
   	       a.price_currentdaymode,
	       a.price_maxmarkdownovertime as currentmarkdown,
	       a.markdown as ismarkeddown
	       
       from ds.all_master_clean as a
       join ds.tmp_agg_date as b
       using (json_productkey, size, batch_date, subcategory, taxonomiccolour)
;

drop view if exists ds.tmp_agg_date cascade;

---------------------------------------------
-- Create a table to be used to run the best/worst seller filters on
-- Performs an aggregation over size from all_agg_date
-- One row per item where an item is defined as a distinct json_productkey+subcategory+colour
---------------------------------------------

drop table if exists ds.seller_master;
create table ds.seller_master as
   select
       json_productkey,
       count(*) as num,
       latest_batch_date,	
       category,
       subcategory,
       retailername,
       facetbrand,
       taxonomiccolour,
       country,
       price_currentdaymode,
       iscoreitem, -- this is currently always set to 0
       newin_7,	
       newin_30,

       -- first date of markdown over all sizes
       min(firstmarkdown_date) as earliestmarkdown,	
       -- highest markdown value over all sizes
       max(currentmarkdown) as maxmarkdown, 
       -- whether item is marked over core size range (zero if there is any markdown in core size range)
       min(case when iscoresize='Y' then ismarkeddown else 999 end) as isallmarkdown_core, 
       -- number of out of stocks over all core sizes
       sum(case when iscoresize='Y' then outofstock_atleastonce else 0 end) as outofstock_core,
       -- percentage of sizes that have been refreshed at least once
       (1.0*sum(wasrefreshed_atleastonce))/(1.0*count(*)) as sum_wasrefreshed

  from ds.all_agg_date  
  group by json_productkey,
           category,
       	   subcategory,
	   latest_batch_date,
       	   retailername,
       	   facetbrand,
	   taxonomiccolour,
       	   country,
       	   price_currentdaymode,
       	   iscoreitem,
       	   newin_7,
       	   newin_30
  distributed randomly;
