/*===========================================================================
* TRG Fashion Analytics Lab 600
*
* Author: Ellie Dobson
* Date:   2014-09-02 
*
* Copyright 2014, Pivotal
*
* DESCRIPTION:
* This script extracts whether the size is core or not from the size field
* 
*/

---------------------------------------------
-- Performs a simple rules based guess as to whether the size is core or not from the size field extracted from the webscrape (which can be messy) 
-- Separate rules for different countries and categories are used
-- More rules can be added to improve the accuracy of the size guess
-- Stores country, category, size and iscoresize stored along with counts in ds.sizetable
---------------------------------------------

drop table if exists ds.sizetable;
create table ds.sizetable as(
select 
       count(*) as num,
       size, 
       country, 
       category, 

       -- words
       case when lower(size) = 's' then 'Y'    
       	    when lower(size) = 'm' then 'Y'    
	    when lower(size) = 'l' then 'Y'    
       	    when lower(size) like '%size s%' then 'Y'    
       	    when lower(size) like '%size m%' then 'Y'    
	    when lower(size) like '%size l%' then 'Y'  
       	    when lower(size) like '%size: s%' then 'Y'    
       	    when lower(size) like '%size: m%' then 'Y'    
	    when lower(size) like '%size: l%' then 'Y'     
	    when lower(size) like '%s/m%' then 'Y'    
	    when lower(size) like '%m/l%' then 'Y'    

	    when lower(size) like '%xl%' then 'N'
	    when lower(size) like '%xs%' then 'N'
	    when lower(size) like '%x l%' then 'N'
	    when lower(size) like '%x s%' then 'N'

       	    when lower(size) like '%small%' then 'Y'    
	    when lower(size) like '%medium%' then 'Y'    
       	    when lower(size) like '%large%' then 'Y'   
       	    when lower(size) like '%regular%' then 'Y' 
       	    when lower(size) like '%reg%' then 'Y'     
       	    when lower(size) like '%avg%' then 'Y'     
       	    when lower(size) like '%one%' then 'Y' 
       	    when lower(size) like '%no%size%' then 'Y' 
       	    when lower(size) like '%petite%' then 'N'
       	    when lower(size) like '%plus%' then 'N'
       	    when lower(size) like '%tall%' then 'N'
       	    when lower(size) like '%long%' then 'N'
       	    when lower(size) like '%short%' then 'N'
       	    when lower(size) like '%age%' then 'N'
       	    when lower(size) like '%months%' then 'N'
       	    when lower(size) like '%years%' then 'N'
       	    when lower(size) like '%Not specified%' then 'not set'
       	    when lower(size) like '%not specified%' then 'not set'

	    -- shoes
	    when size like '%3%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='uk' or country='au') then 'N'
	    when size like '%4%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='uk' or country='au') then 'N'
	    when size like '%5%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='uk' or country='au') then 'Y'
	    when size like '%6%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='uk' or country='au') then 'Y'
	    when size like '%7%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='uk' or country='au') then 'N'
	    when size like '%8%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='uk' or country='au') then 'N'

	    when size like '%4%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='us') then 'N'
	    when size like '%5%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='us') then 'Y'
	    when size like '%6%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='us') then 'Y'
	    when size like '%7%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='us') then 'Y'
	    when size like '%8%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='us') then 'Y'
	    when size like '%9%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='us') then 'Y'
	    when size like '%10%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='us') then 'Y'
	    when size like '%11%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='us') then 'N'
	    when size like '%12%' and (category like '%shoe%' or category like '%boots%' or category like '%sandals%') and (country='us') then 'N'

	    -- jeans and trousers
       	    when lower(size) like '%25%' and (category='jeans' or category='trousers/pants') then 'N'
       	    when lower(size) like '%26%' and (category='jeans' or category='trousers/pants') then 'N'
       	    when lower(size) like '%27%' and (category='jeans' or category='trousers/pants') then 'Y'
       	    when lower(size) like '%28%' and (category='jeans' or category='trousers/pants') then 'Y'
       	    when lower(size) like '%29%' and (category='jeans' or category='trousers/pants') then 'Y'
       	    when lower(size) like '%30%' and (category='jeans' or category='trousers/pants') then 'Y'
       	    when lower(size) like '%31%' and (category='jeans' or category='trousers/pants') then 'Y'
       	    when lower(size) like '%32%' and (category='jeans' or category='trousers/pants') then 'Y'
       	    when lower(size) like '%33%' and (category='jeans' or category='trousers/pants') then 'Y'
       	    when lower(size) like '%34%' and (category='jeans' or category='trousers/pants') then 'Y'
       	    when lower(size) like '%35%' and (category='jeans' or category='trousers/pants') then 'N'
       	    when lower(size) like '%36%' and (category='jeans' or category='trousers/pants') then 'N'

	    -- bras       	    
       	    when lower(size) like '%a' and (category='lingerie/intimates' or category='swim') then 'N'
       	    when lower(size) like '%f' and (category='lingerie/intimates' or category='swim') then 'N'
       	    when lower(size) like '%g' and (category='lingerie/intimates' or category='swim') then 'N'
       	    when lower(size) like '%h' and (category='lingerie/intimates' or category='swim') then 'N'
       	    when lower(size) like '%j' and (category='lingerie/intimates' or category='swim') then 'N'
       	    when lower(size) like '%38%' and (category='lingerie/intimates' or category='swim') then 'N'
       	    when lower(size) like '%40%' and (category='lingerie/intimates' or category='swim') then 'N'
       	    when lower(size) like '%42%' and (category='lingerie/intimates' or category='swim') then 'N'
       	    when lower(size) like '%24%' and (category='lingerie/intimates' or category='swim') then 'N'
       	    when lower(size) like '%26%' and (category='lingerie/intimates' or category='swim') then 'N'

       	    when lower(size) like '%b' and (category='lingerie/intimates' or category='swim') then 'Y'
       	    when lower(size) like '%c' and (category='lingerie/intimates' or category='swim') then 'Y'
       	    when lower(size) like '%d' and (category='lingerie/intimates' or category='swim') then 'Y'
       	    when lower(size) like '%e' and (category='lingerie/intimates' or category='swim') then 'Y'
       	    when lower(size) like '%28%' and (category='lingerie/intimates' or category='swim') then 'Y'
       	    when lower(size) like '%30%' and (category='lingerie/intimates' or category='swim') then 'Y'
       	    when lower(size) like '%32%' and (category='lingerie/intimates' or category='swim') then 'Y'
       	    when lower(size) like '%34%' and (category='lingerie/intimates' or category='swim') then 'Y'
       	    when lower(size) like '%36%' and (category='lingerie/intimates' or category='swim') then 'Y'
	    
	    -- plain sizes
	    -- us
	    when size like '%24%' and (country='us') then 'N'
	    when size like '%26%' and (country='us') then 'N'
	    when size like '%28%' and (country='us') then 'Y'    	  
	    when size like '%30%' and (country='us') then 'Y'    	  	  
	    when size like '%32%' and (country='us') then 'Y'    
	    when size like '%34%' and (country='us') then 'Y'    
	    when size like '%36%' and (country='us') then 'N'
	    when size like '%40%' and (country='us') then 'N'    
	    when size like '%42%' and (country='us') then 'N'

       	    when size like '%10%' and (country='us') then 'Y'  
       	    when size like '%11%' and (country='us') then 'Y'  
	    when size like '%12%' and (country='us') then 'Y'
	    when size like '%13%' and (country='us') then 'N'
	    when size like '%14%' and (country='us') then 'N'
	    when size like '%15%' and (country='us') then 'N'
	    when size like '%16%' and (country='us') then 'N'
	    when size like '%17%' and (country='us') then 'N'
	    when size like '%18%' and (country='us') then 'N'

	    -- now the single figures (needs to go at the end)
	    when size like '%0%' and (country='us') then 'N'
	    when size like '%1%' and (country='us') then 'N'
	    when size like '%2%' and (country='us') then 'N'
	    when size like '%3%' and (country='us') then 'N'
	    when size like '%4%' and (country='us') then 'N'
	    when size like '%5%' and (country='us') then 'Y'
	    when size like '%6%' and (country='us') then 'Y'
	    when size like '%7%' and (country='us') then 'Y'
	    when size like '%8%' and (country='us') then 'Y'
	    when size like '%9%' and (country='us') then 'Y'

	    -- uk
	    -- other size definition
	    when size like '%24%' and (country='uk' or country='au') then 'N'
	    when size like '%26%' and (country='uk' or country='au') then 'N'
	    when size like '%28%' and (country='uk' or country='au') then 'Y'    	  
	    when size like '%30%' and (country='uk' or country='au') then 'Y'    	  	  
	    when size like '%32%' and (country='uk' or country='au') then 'Y'    
	    when size like '%34%' and (country='uk' or country='au') then 'Y'    
	    when size like '%36%' and (country='uk' or country='au') then 'N'
	    when size like '%40%' and (country='uk' or country='au') then 'N'    
	    when size like '%42%' and (country='uk' or country='au') then 'N'

       	    when size like '%10%' and (country='uk' or country='au') then 'Y'    
       	    when size like '%12%' and (country='uk' or country='au') then 'Y'    
       	    when size like '%14%' and (country='uk' or country='au') then 'Y'  
	    when size like '%16%' and (country='uk' or country='au') then 'Y'
	    when size like '%18%' and (country='uk' or country='au') then 'N'
	    when size like '%20%' and (country='uk' or country='au') then 'N'
	    when size like '%22%' and (country='uk' or country='au') then 'N'

	    -- now the rest (needs to go at the end)
	    when size like '%4%' and (country='uk' or country='au') then 'N'
	    when size like '%6%' and (country='uk' or country='au') then 'N'
	    when size like '%8%' and (country='uk' or country='au') then 'N'

      	    else 'not set'

       end as iscoresize
from ds.all_master_clean
group by category, country,  size, iscoresize
)
distributed by (category, country, size)
;
