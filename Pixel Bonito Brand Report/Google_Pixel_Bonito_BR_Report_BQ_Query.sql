/*
Google Pixel Bonito Q2-Q3 - Report
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
    `essence-analytics-dwh.rtf_pixel_brand_report.STAGING_moat_fb_video`
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
    `essence-analytics-dwh.rtf_pixel_brand_report.STAGING_moat_tw_video`
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
    `essence-analytics-dwh.rtf_pixel_brand_report.STAGING_truview`
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
    `essence-analytics-dwh.rtf_pixel_brand_report.STAGING_moat_cm_vid`
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
    `essence-analytics-dwh.rtf_pixel_brand_report.STAGING_moat_tw_disp`
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
    `essence-analytics-dwh.rtf_pixel_brand_report.STAGING_moat_ig_disp`
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
    `essence-analytics-dwh.rtf_pixel_brand_report.STAGING_moat_fb_disp`
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
    `essence-analytics-dwh.rtf_pixel_brand_report.STAGING_moat_cm_disp`
  GROUP BY
    1,
    2 ),
  ------------------- DataLab Export -------------------
  datalab AS (
  SELECT
    Time_1__Date_of_Activity AS date,
    Olive_Plan_Name AS media_plan_name,
    Olive_Plan_Line_ID AS plan_line_id,
    Olive_Plan_Line_Name AS plan_line_name,
    Olive__Plan_Line_Property_ AS property,
    Olive_Plan_Line_Type AS type,
    Olive__Plan_Line_Channel_ AS channel,
    Olive__Placement_ID_ AS opid,
    Performance_And_Delivery_1__Impressions AS datalab_impressions,
    Performance_And_Delivery_2__Clicks AS clicks,
    Performance_And_Delivery_6__Spend_in_USD AS spend_usd
  FROM
    `essence-analytics-dwh.rtf_pixel_brand_report.datalab_export`
  WHERE
    Olive_Plan_Line_Name != "YouTube Masthead") -- Matt Doesn't care about this)
  ------------------- Whole Shebang -------------------
SELECT
  *,
  ifnull(moat_vid_impressions_analyzed,
    0) + ifnull(moat_disp_impressions_analyzed,
    0) AS total_moat_impressions_analyzed
FROM
  datalab
LEFT JOIN
  moat_disp
USING
  (date,
    opid)
LEFT JOIN
  moat_vid
USING
  (date,
    opid)