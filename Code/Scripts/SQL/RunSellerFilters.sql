/*===========================================================================
* TRG Fashion Analytics Lab 600
*
* Author: Ellie Dobson
* Date:   2014-09-02 
*
* Copyright 2014, Pivotal
*
* DESCRIPTION:
* This script runs the filters run to define best and worst sellers
* It creates the tables (ds.extremesellers and ds.extremesellers_lastweek) that Tableau connects to
* NOTE: The hard coded date at the end should be replaced for today() when the script is put into production
*/


---------------------------------------------
-- Assign table where 'best' and 'worst' sellers are marked as such in 'sellers' column
-- Items which are neither best nor worst are marked as 'all'
---------------------------------------------

drop table if exists ds.extremesellers;
create table ds.extremesellers as (
       select 
	      json_productkey,
	      latest_batch_date,
       	      category,
       	      subcategory,
       	      retailername,
       	      facetbrand,
	      taxonomiccolour,
	      country,
       	      price_currentdaymode,
	-- Selection of best sellers
        case when(
       	     -- product is at full price
       	     maxmarkdown=0 and	  
       	     -- have had a short life
       	     newin_30=1 and
       	     -- SKU in the core size range have been out of stock at least once
       	     outofstock_core!=0 and
       	     -- not a core item (eg white tshirt): currently does nothing
	     iscoreitem=1 and
	     -- at least one product refresh over time and size
	     sum_wasrefreshed>0
	)
	then 'best'
	-- Selection of worst sellers
        when(
	     -- marked down across all core sizes
	     isallmarkdown_core=1 and 
   	     -- marked down for 3+ weeks
	     (latest_batch_date-earliestmarkdown) > 21 and
	     -- maximum mark down over 30%
	     maxmarkdown>30
       )
       then 'worst'
       -- Mark everything else as 'other'
       else 'other'
       end as sellers  
       from ds.seller_master

)
distributed randomly
;

---------------------------------------------
-- Create a table as above but only for items that were still available as of last week included
-- The hard coded date should be replaced for today() when the script is put into production
---------------------------------------------

drop table if exists ds.extremesellers_lastweek;
create table ds.extremesellers_lastweek as (
  select 
  	 * 
  from ds.extremesellers
  where latest_batch_date < date'2014-07-23' -'1 weeks'::interval 
)
distributed randomly;


