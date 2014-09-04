create schema src;

drop external table if exists src.ext_product_copper;
create external table src.ext_product_copper (
    batch_date date,
    kapow_product_id varchar,
    scrape_start_date timestamp,
    brand varchar,
    url varchar,
    product_name varchar,
    product_detail varchar,
    product_description varchar,
    retailer_product_reference varchar,
    retailer_name varchar,
    country varchar,
    currency varchar,
    site_url varchar,
    tag varchar,
    detail_collected varchar
)
location ('pxf://hdpvnode04:50070/data/product_copper_new/*.gz?profile=HdfsTextSimple')
format 'text' (delimiter '	' null '' fill missing fields);

drop table if exists src.product_copper;
create table src.product_copper with (appendonly=true, compresstype=zlib, compresslevel=4) as
	select * from src.ext_product_copper
distributed randomly;

drop external table if exists src.ext_product_gold;
create external table src.ext_product_gold (
    batch_date date,
    product_key varchar,
    sku_master_json varchar
)
location ('pxf://hdpvnode04:50070/data/product_gold_srch_docs/*/*/*.gz?profile=HdfsTextSimple')
format 'text' (delimiter '	' null '' FILL MISSING FIELDS);

drop table if exists src.product_gold_raw;
create table src.product_gold_raw with (appendonly=true, compresstype=zlib, compresslevel=4) as
	select * from src.ext_product_gold
distributed randomly;

create or replace function src.product_gold_json_parse_py(json_field varchar) returns varchar[] as $BODY$
import json
try:
    str2 = ''
    next_check = 0
    blabla = 0
    for c in json_field:
        if next_check == 1:
            if str2[-2] not in [':',',','[','{'] and c not in [':',',',']','}']:
                str2 = str2[:-1] + '\\"'
            next_check = 0
        str2 += c
        if c=='"':
            next_check = 1
    j = json.loads(str2)
except:
    j = {}
if j:
    p = j.get('price')
    if p is None:
        p = {'currency':None,
             'maxMarkdownOverTime':None,
             'currentDayMin':None,
             'overTimeMin':None,
             'currentDayMode':None,
             'currentDayMax':None,
             'overTimeMax':None,
             'firstCollectedPrice':None}
    return [
            j.get('productKey'),
            j.get('description'),
            j.get('name'),
            j.get('detail'),
            j.get('retailerName'),
            j.get('sourceSite'),
            j.get('retailerProductUrl'),
            j.get('country'),
            j.get('scrapeDate'),
            j.get('brand'),
            j.get('facetBrand'),
            j.get('rawMaterialComposition'),
            j.get('materialComposition'),
            j.get('division'),
            j.get('department'),
            j.get('category'),
            j.get('subCategory'),
            j.get('pattern'),
            j.get('newInLastSevenDays'),
            j.get('newInLastThirtyDays'),
            j.get('overTimeSkuColours'),
            j.get('outOfStock'),
            j.get('discounted'),
            p.get('currency'),
            p.get('maxMarkdownOverTime'),
            p.get('currentDayMin'),
            p.get('overTimeMin'),
            p.get('currentDayMode'),
            p.get('currentDayMax'),
            p.get('overTimeMax'),
            p.get('firstCollectedPrice'),
            j.get('firstSeenDate'),
            j.get('retailerDisplayName')
        ]
else:
    return None
$BODY$
language plpythonu
volatile;

drop table if exists src.product_gold;
create table src.product_gold with (appendonly=true, compresstype=zlib, compresslevel=4) as
    select  batch_date,
            product_key,
            j[1] as json_productKey,
            j[2] as description,
            j[3] as name,
            j[4] as detail,
            j[5] as retailerName,
            j[6] as sourceSite,
            j[7] as retailerProductUrl,
            j[8] as country,
            j[9] as scrapeDate,
            j[10] as brand,
            j[11] as facetBrand,
            j[12] as rawMaterialComposition,
            j[13] as materialComposition,
            j[14] as division,
            j[15] as department,
            j[16] as category,
            j[17] as subCategory,
            j[18] as pattern,
            j[19] as newInLastSevenDays,
            j[20] as newInLastThirtyDays,
            j[21] as overTimeSkuColours,
            j[22] as outOfStock,
            j[23] as discounted,
            j[24] as price_currency,
            j[25] as price_maxMarkdownOverTime,
            j[26] as price_currentDayMin,
            j[27] as price_overTimeMin,
            j[28] as price_currentDayMode,
            j[29] as price_currentDayMax,
            j[30] as price_overTimeMax,
            j[31] as price_firstCollectedPrice,
            j[32] as firstSeenDate,
            j[33] as retailerDisplayName
        from (
            select  batch_date,
                    product_key,
                    src.product_gold_json_parse_py(sku_master_json) as j
                from src.product_gold_raw
            ) as q
distributed randomly;

drop external table src.ext_sku_copper;
create external table src.ext_sku_copper (
    kapow_sku_id varchar,
    kapow_product_id varchar,
    retailer_product_ref varchar,
    product_name varchar,
    retailer_name varchar,
    retailer_sku_reference varchar,
    sku_product_name varchar,
    price varchar,
    price_text varchar,
    currency varchar,
    raw_colour varchar,
    out_of_stock varchar,
    robot_name varchar,
    last_extracted varchar,
    batch_date varchar,
    country varchar,
    detail_collected varchar
)
location ('pxf://hdpvnode04:50070/data/sku_copper/*/*/*.gz?profile=HdfsTextSimple')
format 'text' (delimiter '	' null '' FILL MISSING FIELDS);

/* !!!running!!! */
drop table if exists src.sku_copper;
create table src.sku_copper with (appendonly=true, compresstype=zlib, compresslevel=4) as
	select * from src.ext_sku_copper
distributed randomly;

drop external table if exists src.ext_sku_master;
create external table src.ext_sku_master (
    skuKey varchar,
    productKey varchar,
    sku_master_json varchar
)
location ('pxf://hdpvnode04:50070/data/sku_master/[au][uks]/*.gz?profile=HdfsTextSimple')
format 'text' (delimiter '	' null '' FILL MISSING FIELDS);

drop table if exists src.sku_master_raw;
create table src.sku_master_raw with (appendonly=true, compresstype=zlib, compresslevel=4) as
	select * from src.ext_sku_master
distributed randomly;

create or replace function src.sku_master_json_parse_main_py(json_field varchar) returns varchar[] as $BODY$
import json
try:
    j = json.loads(json_field)
except:
    j = {}
if j:
    return [
            j.get('skuKey'),
            j.get('country'),
            j.get('currency'),
            j.get('productKey'),
            j.get('retailerName'),
            j.get('latestKapowSkuId'),
            j.get('colourUrl'),
            j.get('size'),
            j.get('colour'),
            j.get('taxonomicColour'),
            j.get('firstSeenDate'),
            j.get('url'),
            j.get('latestAsOfDate')
        ]
else:
    return None
$BODY$
language plpythonu
volatile;

drop table if exists src.sku_master;
create table src.sku_master with (appendonly=true, compresstype=zlib, compresslevel=4) as
    select  skuKey,
            productKey,
            j[1] as json_skuKey,
            j[2] as country,
            j[3] as currency,
            j[4] as json_productKey,
            j[5] as retailerName,
            j[6] as latestKapowSkuId,
            j[7] as colourUrl,
            j[8] as size,
            j[9] as colour,
            j[10] as taxonomicColour,
            j[11] as firstSeenDate,
            j[12] as url,
            j[13] as latestAsOfDate
        from (
            select  skuKey,
                    productKey,
                    src.sku_master_json_parse_main_py(sku_master_json) as j
                from src.sku_master_raw
            ) as q
distributed by (skuKey, productKey);

create or replace function src.sku_master_json_parse_images_py(json_field varchar) returns varchar[] as $BODY$
import json
try:
    j = json.loads(json_field)
except:
    j = {}
if j:
    images = j.get('latestImages')
    if images is None:
        return None
    else:
        res = []
        for x in images:
            res.append('')
            res[-1] += str(x.get('isDefault')) + '^'
            res[-1] += ' & '.join([ '|'.join([xx.get('size'),xx.get('type'),xx.get('filename')]) for xx in x.get('sizes')])
        return res
else:
    return None
$BODY$
language plpythonu
volatile;

drop table if exists src.sku_master_images;
create table src.sku_master_images with (appendonly=true, compresstype=zlib, compresslevel=4) as
    select  skuKey,
            productKey,
            split_part(image, '^', 1) as isDefault,
            split_part(image, '^', 2) as sizes
        from (
            select  skuKey,
                    productKey,
                    unnest(images) as image
                from (
                    select	skuKey,
                            productKey,
                            src.sku_master_json_parse_images_py(sku_master_json) as images
                        from src.sku_master_raw
                    ) as q
            ) as q2
distributed by (skuKey, productKey);

create or replace function src.sku_master_json_parse_skuhistory_py(json_field varchar) returns varchar[] as $BODY$
import json
def xstr(s):
    if s is None:
        return ''
    else:
        return str(s)
try:
    j = json.loads(json_field)
except:
    j = {}
if j:
    history = j.get('history')
    if history is None:
        return None
    else:
        res = []
        for x in history:
            s = '|'.join([
                xstr(x),
                xstr(history[x].get('present')),
                xstr(history[x].get('price')),
                xstr(history[x].get('outOfStock')),
                xstr(history[x].get('kapowSkuId')),
                xstr(history[x].get('SKUMarkDown'))
                ])
            res.append(s)
        return res
else:
    return None
$BODY$
language plpythonu
volatile;

drop table if exists src.sku_master_skuhistory;
create table src.sku_master_skuhistory with (appendonly=true, compresstype=zlib, compresslevel=4) as
    select  skuKey,
            productKey,
            split_part(history, '|', 1)::date as history_date,
            case split_part(history, '|', 2)
                when 'True' then 1
                when 'False' then 0
                else null
            end::smallint as present,
            case split_part(history, '|', 3)
                when '' then null
                else split_part(history, '|', 3)
            end::float8 as price,
            case split_part(history, '|', 4)
                when 'True' then 1
                when 'False' then 0
                else null
            end::smallint as outOfStock,
            split_part(history, '|', 5) as SKUMarkDown
        from (
            select  skuKey,
                    productKey,
                    unnest(history) as history
                from (
                    select	skuKey,
                            productKey,
                            src.sku_master_json_parse_skuhistory_py(sku_master_json) as history
                        from src.sku_master_raw
                    ) as q
            ) as q2
distributed by (skuKey, productKey);
