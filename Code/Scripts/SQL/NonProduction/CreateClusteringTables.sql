-- Table aggregated over size
drop table if exists ds.cluster_master;
create table ds.cluster_master as (
       select 
              retailername,
	      1.0*sum(case when newin_7=1 then 1 else 0 end)/sum(1) as percent_newin7, 
	      count(distinct(facetbrand)) as numbrands,
	      avg(price_currentdaymode) as avprice
       from ds.all_master_clean
       group by 	     
       	      retailername, category, subcategory
)
distributed randomly
;
