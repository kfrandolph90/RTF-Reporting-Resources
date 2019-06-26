/*
Google Pixel Bonito Q2-Q3 - Report
THIS IS A SHITTY QUERY
*/

  ------------------- Union All Video Tables -------------------
WITH
  moat_vid AS (
    -- Moat FB Video -- Tile: 8268
  SELECT
    date,
    CAST(REGEXP_EXTRACT(level4_label,r"OPID-(\d+)") AS int64) AS opid,
    SUM(impressions_analyzed) AS moat_vid_impressions_analyzed,
    SUM(susp_valid) AS moat_vid_susp_valid,
    SUM(valid_and_viewable) AS valid_and_viewable,
    SUM(reached_first_quart_sum) reached_first_quart_sum,
    SUM(reached_second_quart_sum) reached_second_quart_sum,
    SUM(reached_third_quart_sum) reached_third_quart_sum,
    SUM(reached_complete_sum) reached_complete_sum,
    SUM(player_visible_on_complete_sum) player_visible_on_complete_sum,
    SUM(player_audible_on_complete_sum) player_audible_on_complete_sum,
    SUM(player_vis_and_aud_on_complete_sum) player_vis_and_aud_on_complete_sum,
    SUM(susp_valid_and_inview_gm_meas_sum) susp_valid_and_inview_gm_meas_sum
  FROM
    `essence-analytics-dwh.rtf_pixel_brand_report.moat_fb_vid`
  GROUP BY
    1,
    2
  UNION ALL
    -- Moat TW Video -- Tile: 6195543
  SELECT
    date,
    CAST(REGEXP_EXTRACT(level2_label,r"OPID-(\d+)") AS int64) AS opid,
    SUM(impressions_analyzed) AS impressions_analyzed,
    SUM(susp_valid) AS susp_valid,
    SUM(valid_and_viewable) AS valid_and_viewable,
    SUM(reached_first_quart_sum) reached_first_quart_sum,
    SUM(reached_second_quart_sum) reached_second_quart_sum,
    SUM(reached_third_quart_sum) reached_third_quart_sum,
    SUM(reached_complete_sum) reached_complete_sum,
    SUM(player_visible_on_complete_sum) player_visible_on_complete_sum,
    SUM(player_audible_on_complete_sum) player_audible_on_complete_sum,
    SUM(player_vis_and_aud_on_complete_sum) player_vis_and_aud_on_complete_sum,
    SUM(susp_valid_and_inview_gm_meas_sum) susp_valid_and_inview_gm_meas_sum
  FROM
    `essence-analytics-dwh.rtf_pixel_brand_report.moat_tw_vid`
  GROUP BY
    1,
    2
  UNION ALL
    -- Moat TrueView -- Tile: 6195541
  SELECT
    date,
    CAST(REGEXP_EXTRACT(level4_label,r"OPID-(\d+)") AS int64) AS opid,
    SUM(impressions_analyzed) AS impressions_analyzed,
    SUM(susp_valid) AS susp_valid,
    SUM(valid_and_viewable) AS valid_and_viewable,
    SUM(reached_first_quart_sum) reached_first_quart_sum,
    SUM(reached_second_quart_sum) reached_second_quart_sum,
    SUM(reached_third_quart_sum) reached_third_quart_sum,
    SUM(reached_complete_sum) reached_complete_sum,
    SUM(player_visible_on_complete_sum) player_visible_on_complete_sum,
    SUM(player_audible_on_complete_sum) player_audible_on_complete_sum,
    SUM(player_vis_and_aud_on_complete_sum) player_vis_and_aud_on_complete_sum,
    SUM(susp_valid_and_inview_gm_meas_sum) susp_valid_and_inview_gm_meas_sum
  FROM
    `essence-analytics-dwh.rtf_pixel_brand_report.moat_yt_trv`
  GROUP BY
    1,
    2
  UNION ALL
    -- Moat Google Vid  -- Tile: 2698
  SELECT
    date,
    CAST(REGEXP_EXTRACT(level3_label,r"OPID-(\d+)") AS int64) AS opid,
    SUM(impressions_analyzed) AS impressions_analyzed,
    SUM(susp_valid) AS susp_valid,
    SUM(valid_and_viewable) AS valid_and_viewable,
    SUM(reached_first_quart_sum) reached_first_quart_sum,
    SUM(reached_second_quart_sum) reached_second_quart_sum,
    SUM(reached_third_quart_sum) reached_third_quart_sum,
    SUM(reached_complete_sum) reached_complete_sum,
    SUM(player_visible_on_complete_sum) player_visible_on_complete_sum,
    SUM(player_audible_on_complete_sum) player_audible_on_complete_sum,
    SUM(player_vis_and_aud_on_complete_sum) player_vis_and_aud_on_complete_sum,
    SUM(susp_valid_and_inview_gm_meas_sum) susp_valid_and_inview_gm_meas_sum
  FROM
    `essence-analytics-dwh.rtf_pixel_brand_report.moat_google_vid`
  GROUP BY
    1,
    2 ),
  ------------------- Union All Display Tables -------------------
  moat_disp AS (
    -- Moat TW Disp -- Tile: 6195541
  SELECT
    date,
    CAST(REGEXP_EXTRACT(level2_label,r"OPID-(\d+)") AS int64) AS opid,
    SUM(impressions_analyzed) AS moat_disp_impressions_analyzed,
    SUM(susp_valid) AS moat_dis_susp_valid,
    -- does this tile have valid imps?
    NULL AS iva
  FROM
    `essence-analytics-dwh.rtf_pixel_brand_report.moat_tw_disp`
  GROUP BY
    1,
    2
  UNION ALL
    -- Moat IG Disp -- Tile: 6188035
  SELECT
    date,
    CAST(REGEXP_EXTRACT(level4_label,r"OPID-(\d+)") AS int64) AS opid,
    SUM(impressions_analyzed) AS impressions_analyzed,
    SUM(valid_and_viewable) AS valid_and_viewable,
    SUM(iva) AS iva
  FROM
    `essence-analytics-dwh.rtf_pixel_brand_report.moat_ig_disp`
  GROUP BY
    1,
    2
  UNION ALL
    -- Moat FB Disp -- Tile: 6195503
  SELECT
    date,
    CAST(REGEXP_EXTRACT(level4_label,r"OPID-(\d+)") AS int64) AS opid,
    SUM(impressions_analyzed) AS impressions_analyzed,
    SUM(valid_and_viewable) AS valid_and_viewable,
    SUM(iva) AS iva
  FROM
    `essence-analytics-dwh.rtf_pixel_brand_report.moat_fb_disp`
  GROUP BY
    1,
    2
  UNION ALL
    -- Moat CM Disp -- Tile: 2506
  SELECT
    date,
    CAST(REGEXP_EXTRACT(level3_label,r"OPID-(\d+)") AS int64) AS opid,
    SUM(impressions_analyzed) AS impressions_analyzed,
    SUM(valid_and_viewable) AS valid_and_viewable,
    SUM(iva) AS iva
  FROM
    `essence-analytics-dwh.rtf_pixel_brand_report.moat_google_disp`
  GROUP BY
    1,
    2 ),
  ------------------- Join Plan Line, aggregate and union onto YT pull -------------------
  moat_vid_line as (  
  select 
  date,
  olive_meta.Olive_Plan_Line_ID as plan_line_id,
  sum(moat_vid_impressions_analyzed) as moat_vid_impressions_analyzed,
  SUM(moat_vid_susp_valid) AS moat_vid_susp_valid,
    SUM(valid_and_viewable) AS valid_and_viewable,
    SUM(reached_first_quart_sum) reached_first_quart_sum,
    SUM(reached_second_quart_sum) reached_second_quart_sum,
    SUM(reached_third_quart_sum) reached_third_quart_sum,
    SUM(reached_complete_sum) reached_complete_sum,
    SUM(player_visible_on_complete_sum) player_visible_on_complete_sum,
    SUM(player_audible_on_complete_sum) player_audible_on_complete_sum,
    SUM(player_vis_and_aud_on_complete_sum) player_vis_and_aud_on_complete_sum,
    SUM(susp_valid_and_inview_gm_meas_sum) susp_valid_and_inview_gm_meas_sum
    from moat_vid
    
    LEFT JOIN  `essence-analytics-dwh.rtf_pixel_brand_report.STAGING_olive_plan_placement` olive_meta
  on moat_vid.opid = olive_meta.Olive_Olive_Placement_ID
    group by 1,2
    
    
  UNION ALL 
  
  SELECT
  date,
  CASE
    WHEN level2_id = 2520197687 THEN 84988
    WHEN level2_id = 2549731844 THEN 84989
    WHEN level2_id = 2550298543 THEN 85312
    WHEN level2_id = 2547410762 THEN 84863
END
  AS plan_line_id,
  SUM(impressions_analyzed) AS moat_vid_impressions_analyzed,
  SUM(susp_valid) AS moat_vid_susp_valid,
  SUM(valid_and_viewable) AS valid_and_viewable,
  SUM(reached_first_quart_sum) reached_first_quart_sum,
  SUM(reached_second_quart_sum) reached_second_quart_sum,
  SUM(reached_third_quart_sum) reached_third_quart_sum,
  SUM(reached_complete_sum) reached_complete_sum,
  SUM(player_visible_on_complete_sum) player_visible_on_complete_sum,
  SUM(player_audible_on_complete_sum) player_audible_on_complete_sum,
  SUM(player_vis_and_aud_on_complete_sum) player_vis_and_aud_on_complete_sum,
  SUM(susp_valid_and_inview_gm_meas_sum) susp_valid_and_inview_gm_meas_sum
FROM
  `essence-analytics-dwh.rtf_pixel_brand_report.moat_yt_res`
GROUP BY
  1,
  2
),
    
  ------------------- Join Plan Line and Aggregate Moat Disp -------------------
  moat_disp_line as (
  select 
   date,
  olive_meta.Olive_Plan_Line_ID as plan_line_id,
  sum(moat_disp_impressions_analyzed) as moat_disp_impressions_analyzed,
  sum(moat_dis_susp_valid) as moat_dis_susp_valid
  from moat_disp
  LEFT JOIN  `essence-analytics-dwh.rtf_pixel_brand_report.STAGING_olive_plan_placement` olive_meta
  on moat_disp.opid = olive_meta.Olive_Olive_Placement_ID
  group by 1,2),

 ------------------- DataLab Export -------------------
  datalab AS (
  SELECT
    Time_1__Date_of_Activity AS date,
    Olive_Plan_Line_ID AS plan_line_id,
    Olive_Plan_Line_Name AS plan_line_name,
    Olive__Plan_Line_Property_ AS property,
    Olive_Plan_Line_Type AS type,
    Olive__Plan_Line_Channel_ AS channel,
    sum(Performance_And_Delivery_1__Impressions) AS datalab_impressions,
    sum(Performance_And_Delivery_2__Clicks) AS clicks,
    sum(Performance_And_Delivery_6__Spend_in_USD) AS spend_usd
  FROM
    `essence-analytics-dwh.rtf_pixel_brand_report.datalab_export`
  WHERE
    Olive_Plan_Line_Name != "YouTube Masthead"
    group by 1,2,3,4,5,6) -- Matt Doesn't care about this)


 ------------------- Whole Shebang -------------------
 
 select *,ifnull(moat_vid_impressions_analyzed,
    0) + ifnull(moat_disp_impressions_analyzed,
    0) AS total_moat_impressions_analyzed
 from datalab 
 left join moat_disp_line using(date,plan_line_id) 
 left join moat_vid_line using(date,plan_line_id)