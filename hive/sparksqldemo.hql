select SUM(revenue) / 100000 as adcost , SUM(winprice_total) / 1000000 AS pubincome ,
SUM(valid_show_count) as ns ,
SUM(case when adspaceslot <= 1 then valid_show_count else 0L end) as p_ns ,
SUM(valid_click_count) as nc , publisher_id,adspace_id from mediav_base.d_clickvalue
where date='2015-05-10' AND solution_entity = 'galileo' AND ad_container != 'APP'
AND advertiser_id > 0 AND campaign_id > 0 AND solution_id > 0 AND banner_id > 0
AND exchanger > 0 AND domain_info.name='(null)'
group by publisher_id, adspace_id limit 10