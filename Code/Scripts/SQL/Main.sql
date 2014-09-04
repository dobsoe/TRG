/*===========================================================================
* TRG Fashion Analytics Lab 600
*
* Author: Ellie Dobson
* Date:   2014-09-02 
*
* Copyright 2014, Pivotal
*
* DESCRIPTION:
* This script runs all SQL scripts to process the data to the form required by the Tableau analysis front end
* NOTE: Running this script will cause large joins to be deleted and remade and will take several hours to run. Don't run this script unless you know what you are doing!
* 
* Input tables: src.product_gold, src.sku_master
* Output tables: ds.extreme_sellers, ds.extreme_sellers_lastweek, ds. seller_master, ds.trends_master, ds.trends_master_weekly, ds.all_master_clean, ds.sizetable
* Output views: ds.product_gold_clean, ds.sku_master_clean, ds.all_agg_date
*/

---------------------------------------------
-- Extract iscoresize fields from size field 
-- Store size category, country, field and iscoresize in ds.sizetable
---------------------------------------------
\i ProcessSize.sql

---------------------------------------------
-- Cleans src.product_gold and src.sku_master
-- Join the above tables on json_product key as well as with ds.sizetable to create ds.all_master_clean
---------------------------------------------
\i BasicCleaning.sql

---------------------------------------------
-- Creates the table on which the sellers filters should be run to define best and worst sellers
-- Processes ds.all_master_clean to create ds.sellermaster
---------------------------------------------
\i CreateSellerTables.sql

---------------------------------------------
-- Runs the filters to define best and worst sellers and tables to which Tableau connects
-- Runs on ds.all_master_clean to create ds.extremesellers and ds.extremesellers_weekly
---------------------------------------------
\i RunSellerFilters.sql

---------------------------------------------
-- Runs the filters to create the trends tables to which Tableau connects
-- Processes ds.all_master_clean to create ds.trends_master and ds.trends_master_weekly
---------------------------------------------
\i CreateTrendsTables.sql

