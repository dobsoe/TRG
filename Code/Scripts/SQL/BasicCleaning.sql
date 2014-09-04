/*===========================================================================
* TRG Fashion Analytics Lab 600
*
* Author: Ellie Dobson
* Date:   2014-09-02 
*
* Copyright 2014, Pivotal
*
* DESCRIPTION:
* This script cleans the data extracted from the webscrape and joins the main two tables to create a master table
* It uses src.product_gold_clean, src.sku_master and ds.sizetable as input, and creates ds.all_master_clean as output
*
* NOTE: The script creates a very large master table and it is worth checking whether the database has capacity for it before running this if the size of src.product_master or src.sku_master has increased
*
* NOTE 2: Once the script is put into production, the maxdate constraint should be removed
* For faster running in production this script could be adapted to add only the newest scraps to the existing master_clean by removing maxdate, setting mindate to the last date the script was run and adding to ds.all_master_clean rather than remaking it
* 
*/

---------------------------------------------
-- Create cleaned view ds.product_gold_clean from product_gold
---------------------------------------------

-- Define the date range we are interested in
\set mindate date'2013-11-01'
\set maxdate date'2014-07-23'

drop view if exists ds.product_gold_clean cascade;
create view ds.product_gold_clean as(
       select
       	      json_productkey, 
	      batch_date,
	      price_maxmarkdownovertime, 
	      -- convert string for markdown to binary
	      case when 
	          upper(markdown)='N' then 0
	      else 1
	      end as markdown,

	      -- convert string for newins to binary
	      case when 
	          upper(newinlastsevendays)='N' then 0
	      else 1
	      end as newin_7,

	      case when 
	          upper(newinlastthirtydays)='N' then 0
	      else 1
	      end as newin_30,

	      -- convert string for out of stock to binary	      
	      case when 
	          outofstock='n' then 0
	      else 1
	      end as outofstock,	 

	      -- convert strings to lowercase
	      lower(category) as category, 

	      -- convert multiple subcategories into multiple entries and convert to lowercase
	      lower(regexp_split_to_table(subcategory, E'\\|'))  as subcategory,

	      -- dummy variable for core items
	      1 as iscoreitem,

	      -- convert retailername to lowercase	      
	      lower(retailerdisplayname) as retailername,

	      -- replace brand entries with 'Not found' where brand is stored as retailer (webscrape failed)
	      case when
	      	  upper(facetbrand)!=upper(retailerdisplayname) then lower(facetbrand)
	      else 'Not found'
	      end as facetbrand,
   	      price_currentdaymode

       from ds.product_gold	
       
       -- cleaning cuts

       -- could put a cut of not null on any variable apart from product_key, choose category. This removes null lines in the database
       where category is not null
       -- only take data from 01 Nov 2013 to 23 July 2014
       and batch_date>=:mindate
       and batch_date<=:maxdate
)
; 


---------------------------------------------
-- Create cleaned view ds.sku_master_clean from sku_master
---------------------------------------------

drop view if exists ds.sku_master_clean;
create view ds.sku_master_clean as (
       select	
		json_productkey,
		-- convert country to lowercase
	      	lower(country) as country,
		-- convert colour to lowercase
	      	lower(taxonomiccolour) as taxonomiccolour,
	      	size
       from ds.sku_master 

       -- could put a cut of not null on any variable apart from product_key, choose category. This removes null lines in the database
       where colour is not null
       group by json_productkey, 
		country, 
                retailername, 
       		size, 
       		taxonomiccolour
)
;


---------------------------------------------
-- Join the two cleaned tables on json_productkey
---------------------------------------------
drop view if exists ds.all_master_clean_tmp cascade;
create view ds.all_master_clean_tmp as 
       select 
       	      *
       from ds.product_gold_clean
       join 
       ds.sku_master_clean
       using(json_productkey)
;


---------------------------------------------
-- Join the all_master_clean_tmp with the size table to create the master table
---------------------------------------------
truncate ds.all_master_clean; 
insert into ds.all_master_clean
       select 
	      json_productkey,
	      country, 
	      batch_date, 
	      category, 
	      subcategory, 
	      taxonomiccolour, 
	      iscoresize, 
	      price_maxmarkdownovertime,
	      price_currentdaymode
	      markdown, 
	      newin_7, 
	      newin_30,
	      outofstock, 
	      iscoreitem, 
	      retailername, 
	      facetbrand 
       from ds.all_master_clean_tmp
       join ds.sizetable as s
       using (size, category, country)
; 
analyze ds.all_master_clean; 

-- Clean up the view that is no longer needed
drop view if exists ds.all_master_clean_tmp cascade;
