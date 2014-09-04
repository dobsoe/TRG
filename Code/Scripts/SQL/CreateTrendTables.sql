/*===========================================================================
* TRG Fashion Analytics Lab 600
*
* Author: Ellie Dobson
* Date:   2014-09-02 
*
* Copyright 2014, Pivotal
*
* DESCRIPTION:
* This script processes ds.all_master_clean to create the table ds.trends_master_weekly that Tableau connects to
* ds.trends_master is also created which has one entry per day but this is too large for Tableau to quickly query it
* 
*/

---------------------------------------------
-- Create cleaned view ds.trends_master from ds.all_master_clean by aggregating over size
-- One row per item where item is defined as json_productkey+retailer+subcategory+colour
--------------------------------------------

-- Table aggregated over size
drop table if exists ds.trends_master;
create table ds.trends_master as (
       select 		      
       	      -- number of distinct sizes		     
              count(distinct(json_productkey)) as numitems, 
	      country,
       	      batch_date,
       	      category,
       	      subcategory,
	      taxonomiccolour,
	      markdown,
	      newin_7, 
	      newin_30,
       	      retailername
       from ds.all_master_clean
       group by 	      
       	      country,
       	      batch_date,
       	      category,
       	      subcategory,
	      taxonomiccolour,
	      markdown,
	      newin_7, 
	      newin_30,
       	      retailername
)
distributed randomly
;


---------------------------------------------
-- Aggregate the above per week for faster (and less noisy) Tableau performance/results
--------------------------------------------

drop table if exists ds.trends_master_weekly;
create table ds.trends_master_weekly as (
       select 
              sum(numitems) as numitems, 
	      to_char(batch_date, 'YYYY.WW') as batch_week,
	      --(extract (year from batch_date) || '-' || extract (week from batch_date)) as batch_week,
	      --to_char('YYYY-WW', batch_date) as batch_week2,   
	      country,
       	      category,
       	      subcategory,
	      taxonomiccolour,
	      markdown,
	      newin_7, 
	      newin_30,
       	      retailername
       from ds.trends_master
       group by 	      
       	      batch_week,	      
       	      country,
       	      category,
       	      subcategory,
	      taxonomiccolour,
	      markdown,
	      newin_7, 
	      newin_30,
       	      retailername
)
distributed randomly
;
