SELECT

  --Staging use only
  is_realtime,
  table_suffix,

  --Time fields
  FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', DATETIME(TIMESTAMP_MILLIS(posix_timestamp_msec), 'Europe/London')) AS activity_date_time,
  EXTRACT(DATE FROM DATETIME(TIMESTAMP_MILLIS(posix_timestamp_msec), 'Europe/London')) AS activity_date,
  EXTRACT(HOUR FROM DATETIME(TIMESTAMP_MILLIS(posix_timestamp_msec), 'Europe/London')) AS activity_hour,  
  FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', LEAD(DATETIME(TIMESTAMP_MILLIS(posix_timestamp_msec), 'Europe/London'), 1) OVER (session_hits_asc)) AS next_date_time,
  
  --Visitor/Session columns
  H.visitor_id AS visitor_id,
  '4' AS visitor_id_type,
  H.session_id AS session_id,  
  CAST(H.session_count AS STRING) AS session_count,
  H.session_hit_count AS session_hit_count,
  COUNTIF(H.hit_type = 'PAGE') OVER(session_hits_asc) AS session_page_view_count,
  
  CASE
    WHEN H.ga_dimension_101 LIKE '%null' THEN NULL
    ELSE REGEXP_EXTRACT(H.ga_dimension_101, r'(?i)CPN[\-:]([0-9A-Za-z\-]+)')
  END AS customer_id,
  '' AS subscription_status_code,
  H.ga_dimension_100 AS customer_type,
  
  H.ga_dimension_102 AS tealium_id,
  
  IF(hit_type = 'PAGE', '0', '100') AS event_type,
  '' AS event_list,
  '' AS event_index_id,
  
  IF(H.event_category = 'user interactions' AND H.event_action = 'post comment', 1, 0) AS is_comment,
  IF(H.event_category = 'media events' AND H.event_action = 'media play' AND H.ga_dimension_137 = '0-25', 1, 0) AS is_video_play,
  IF(H.event_category = 'media events' AND H.event_action = 'media complete' AND H.ga_dimension_137 = '76-100', 1, 0) AS is_video_complete,
  IF(H.event_category = 'social share events' AND (H.event_action IN ('share start', 'share button', 'social share start')), 1, 0) AS is_share,
  IF(H.event_category = 'registration and subscription events' AND H.event_action = 'registration', 1, 0) is_registration,
  IF(H.event_category = 'registration and subscription events' AND H.event_action = 'subscription', 1, 0) is_subscription,  
  IF(H.event_action IN ('add to my articles', 'add article to my articles'), 1, 0) AS is_save_add,  
  IF(H.event_action IN ('remove from my articles', 'remove article from my articles'), 1, 0) AS is_save_remove,
  
  IF(H.hit_type = 'PAGE', 1, 0) AS is_page_view,
  
  '' AS auto_download,
  
  CAST(NULL AS FLOAT64) AS time_spent_secs, --Backfilled on update
  
  H.ga_dimension_1 AS product,
  P.product_version,
  P.product_type, 
  P.application_type AS product_application_type,    
  H.ga_dimension_115 AS product_software_version, 

    
  H.url AS url,
  H.url_domain_name AS url_domain_name,
  H.ga_dimension_2 AS page_name,
  H.ga_dimension_7 AS page_type,
  H.ga_dimension_30 AS element_clicked_name,
  
  CASE 
    WHEN H.ga_dimension_30 LIKE ('%share%facebook%') THEN 'Facebook'
    WHEN H.ga_dimension_30 LIKE ('%share%twitter%') THEN 'Twitter'
    WHEN H.ga_dimension_30 LIKE ('%share%mail%') THEN 'Email'
  END social_share_type,
  
  H.ga_dimension_3 AS section,
  H.ga_dimension_4 AS section_level2,
  H.ga_dimension_51 AS content_id,
  H.ga_dimension_53 AS content_headline,
  H.ga_dimension_54 AS content_author,
  H.ga_dimension_63 AS content_headline_detail,
  
  H.ga_dimension_58 AS editorial_tag,
 
  CONCAT(REPLACE(REGEXP_EXTRACT(H.ga_dimension_55, r'(\d+/\d+/\d+ \d+:\d+)'),'/','-'),':00') AS published_date_time,
  REPLACE(REGEXP_EXTRACT(H.ga_dimension_55, r'^(\d+/\d+/\d+).+'),'/','-') AS published_date,
  CONCAT(REGEXP_EXTRACT(H.ga_dimension_55, r'^(?:\d+/\d+/\d+) (\d+:\d+).+'),':00') AS published_time,    
  REGEXP_EXTRACT(H.ga_dimension_55, r'([a-z,A-Z]+)') AS published_day_of_week,    
  
  CONCAT(
    IFNULL(CONCAT(H.utm_campaign,':'), ''), 
    IFNULL(CONCAT(H.utm_term, ':'), ''),
    IFNULL(CONCAT(H.utm_medium, ':'), ''),
    IFNULL(CONCAT(H.utm_source, ':'), ''),
    IFNULL(H.utm_content, '')
  ) AS marketing_tracking_code,
  FIRST_VALUE(CONCAT(
    IFNULL(CONCAT(H.utm_campaign,':'), ''), 
    IFNULL(CONCAT(H.utm_term, ':'), ''),
    IFNULL(CONCAT(H.utm_medium, ':'), ''),
    IFNULL(CONCAT(H.utm_source, ':'), ''),
    IFNULL(H.utm_content, '')
  )) OVER(session_hits_asc) AS marketing_tracking_code_session,
  
  '' AS ip,
  H.ip_domain_name,

  C.isoalpha3 AS country_code,
  H.geo_country AS country_name,
  R.region_code AS region_code,
  H.geo_region AS region_name,
  H.geo_city AS city,
  '' AS postal_code,  

  H.ga_dimension_111 AS user_agent,
  
  H.operating_system AS operating_system,
  H.operating_system_version AS operating_system_version,
  
  H.mobile_device_info AS device,
  H.device_category AS device_type,
  
  H.referral_path AS referrer_first_ever,
  '' AS referrer_type_first_ever,
  FIRST_VALUE(H.ga_dimension_123) OVER (session_hits_asc) AS referrer_session,
  
  FIRST_VALUE(CASE 
    WHEN H.ga_dimension_123 IS NULL AND H.session_hit_count = 1 THEN 'Typed/Bookmarked'
    WHEN H.ga_dimension_123 IS NOT NULL AND RT.domain_type IS NULL THEN 'Unknown'
    ELSE RT.domain_type
  END) OVER(session_hits_asc) AS referrer_type_session,  

  H.ga_dimension_123 AS referrer_hit,
  
  CASE 
    WHEN H.ga_dimension_123 IS NULL AND H.session_hit_count = 1 THEN 'Typed/Bookmarked'
    WHEN H.ga_dimension_123 IS NOT NULL AND RT.domain_type IS NULL THEN 'Unknown'
    ELSE RT.domain_type
  END AS referrer_type_hit,
  
  LAG(H.ga_dimension_2, 1) OVER (session_hits_asc) AS previous_hit_page_name,
  LAG(H.ga_dimension_51, 1) OVER (session_hits_asc) AS previous_hit_content_id,
  LEAD(H.ga_dimension_2, 1) OVER (session_hits_asc) AS next_hit_page_name,
  LEAD(H.ga_dimension_51, 1) OVER (session_hits_asc) AS next_hit_content_id,  

  CASE
    WHEN LEAD(H.posix_timestamp_msec, 1) OVER (session_hits_asc) IS NULL THEN 1
    ELSE 0
  END AS is_last_page_of_session, 

  CAST(NULL AS INT64) AS pre_registration_article_hit,
  CAST(NULL AS INT64) AS pre_subscription_article_hit,
  MIN(CASE WHEN H.event_category = 'registration and subscription events' AND H.event_action = 'registration' THEN H.session_hit_count END) OVER (session_hits_asc) AS registration_session_hit_num_min,  
  MIN(CASE WHEN H.event_category = 'registration and subscription events' AND H.event_action = 'subscription' THEN H.session_hit_count END) OVER (session_hits_asc) subscription_session_hit_num_min, 
  CAST(NULL AS INT64) AS pre_registration_article_session_hit_count_max,
  CAST(NULL AS INT64) AS pre_subscription_article_session_hit_count_max,

  H.ga_dimension_103 AS third_party_id,
  H.ga_dimension_104 AS news_corp_id,

  IF(H.event_category = 'social share events' AND H.event_action = 'share complete', 1, 0) AS is_share_complete,

  IF(H.event_category = 'user interactions' AND H.event_action = 'code entry: success', 1, 0) AS is_sun_savers_code_banked,

  CONCAT(REPLACE(SUBSTR(H.ga_dimension_109, 1, 16), '/', '-'), ':00') AS user_local_time,

  H.ga_dimension_105 AS tealium_session_id,
  H.ga_dimension_106 AS tealium_session_number,
  H.ga_dimension_107 AS tealium_session_page_view_count,
  H.ga_dimension_108 AS tealium_session_hit_count,  
  
  H.utm_campaign AS utm_campaign,
  H.utm_source AS utm_source,
  H.utm_medium AS utm_medium,
  H.utm_term AS utm_term,
  H.utm_content AS utm_content, 

  H.channel_grouping AS channel_grouping,

  CAST(H.is_ga_true_direct AS STRING) AS is_ga_true_direct,	
  
  H.ga_dimension_113 AS device_orientation,

  P.product_group AS product_group,
  P.platform_type AS platform_type,
  P.deprecated_or_ignore AS deprecated_or_ignore, 
  
  H.event_category AS event_category,
  H.event_action AS event_action,
  H.event_label AS event_label,
  H.event_value AS event_value,  
  
  H.ga_dimension_31 AS event_interaction_method,  
  H.ga_dimension_32 AS event_interaction_details_1,   
  H.ga_dimension_33 AS event_interaction_details_2,   
  H.ga_dimension_34 AS event_interaction_details_3,     

  H.ga_dimension_126 AS experiment_details,
  
  H.posix_timestamp_msec AS posix_timestamp_msec,
  
  H.device_browser AS device_browser,
  
  H.ga_dimension_11 AS previous_page_site_section,
  
  CAST(NULL AS FLOAT64) AS page_dwell_time_seconds,
  
  CASE 
    -- WHEN H.utm_source LIKE 'facebook%' THEN REGEXP_EXTRACT(H.utm_campaign, r'([a-zA-Z]{1,})') 
    WHEN H.url LIKE '%utm_source=facebook%' 
    THEN REGEXP_EXTRACT(H.url, r"utm_campaign=([a-zA-Z_]{1,})")
    ELSE NULL
  END AS facebook_page,
  CASE 
    -- WHEN H.utm_source LIKE 'facebook%' THEN SAFE.PARSE_DATE('%d%m%y', REGEXP_EXTRACT(H.utm_campaign, r'[a-z]{1,}([0-9]{6})'))
    WHEN REGEXP_CONTAINS(REGEXP_EXTRACT(H.url, r"utm_campaign=([a-zA-Z]{1,}[0-9]{6})"),'[0-9]')
    THEN CAST(FORMAT_DATE("%Y-%m-%d", SAFE.PARSE_DATE('%d%m%y',  REGEXP_EXTRACT(H.url, r"utm_campaign=[a-z]{1,}([0-9]{6})"))) AS date)
    ELSE NULL 
  END AS facebook_post_date,

  -- FIRST_VALUE(H.utm_campaign) OVER(session_hits_asc) AS utm_campaign_last_click,
  -- FIRST_VALUE(H.utm_source) OVER(session_hits_asc) AS utm_source_last_click,
  -- FIRST_VALUE(H.utm_medium) OVER(session_hits_asc) AS utm_medium_last_click,
  -- FIRST_VALUE(H.utm_term) OVER(session_hits_asc) AS utm_term_last_click,
  -- FIRST_VALUE(H.utm_content) OVER(session_hits_asc) AS utm_content_last_click,
  FIRST_VALUE(
    IF(H.url LIKE "%utm_campaign%", REGEXP_EXTRACT(H.url, r"utm_campaign=([a-zA-Z0-9_+-]{1,})"), NULL)) 
  OVER (session_hits_asc) AS utm_campaign_last_click,
  FIRST_VALUE(
    IF(H.url LIKE "%utm_source%", REGEXP_EXTRACT(H.url, r"utm_source=([a-zA-Z0-9_+-]{1,})"), NULL))
  OVER (session_hits_asc) AS utm_source_last_click,
  FIRST_VALUE(
    IF(H.url LIKE "%utm_medium%", REGEXP_EXTRACT(H.url, r"utm_medium=([a-zA-Z0-9_+-]{1,})"), NULL))
  OVER (session_hits_asc) AS utm_medium_last_click,
  FIRST_VALUE(
    IF(H.url LIKE "%utm_term%", REGEXP_EXTRACT(H.url, r"utm_term=([a-zA-Z0-9_+-]{1,})"), NULL))
  OVER (session_hits_asc) AS utm_term_last_click,
  FIRST_VALUE(
    IF(H.url LIKE "%utm_content%", REGEXP_EXTRACT(H.url, r"utm_content=([a-zA-Z0-9_+-]{1,})"), NULL)) 
  OVER (session_hits_asc) AS utm_content_last_click,
  
  CASE
    WHEN H.ip_domain_name IN ('amazonaws.com', 'googleusercontent.com','azure.com','linode.com','vultr.com','your-server.de') THEN 'cloud computing platform'
    WHEN H.ip_domain_name in ('newsint.co.uk', 'newscorp.com', 'news.co.uk') THEN 'news owned domain' 
    ELSE 'other'
  END as ip_domain_name_type,     

  CASE
    WHEN H.ga_dimension_1 = 'scottish sun' THEN CONCAT('thescottishsun_', H.ga_dimension_51)
    WHEN H.ga_dimension_1 = 'scottish sun amp' THEN CONCAT('thescottishsun_', H.ga_dimension_51)
    WHEN H.ga_dimension_1 = 'irish sun' THEN CONCAT('theirishsun_', H.ga_dimension_51)
    WHEN H.ga_dimension_1 = 'irish sun amp' THEN CONCAT('theirishsun_', H.ga_dimension_51)
    WHEN H.ga_dimension_1 = 'the sun us' THEN CONCAT('theussun_', H.ga_dimension_51)
    WHEN H.ga_dimension_1 = 'the sun us amp' THEN CONCAT('theussun_', H.ga_dimension_51)
    WHEN H.ga_dimension_1 = 'the sun us facebook instant articles' THEN CONCAT('theussun_', H.ga_dimension_51)
    ELSE CONCAT('thesun_', H.ga_dimension_51)
  END AS content_cms_id,

  DATETIME(TIMESTAMP_MILLIS(H.posix_timestamp_msec), 'US/Eastern') AS us_date_time,
  EXTRACT(DATE FROM DATETIME(TIMESTAMP_MILLIS(H.posix_timestamp_msec), 'US/Eastern')) AS us_date,
  EXTRACT(HOUR FROM DATETIME(TIMESTAMP_MILLIS(H.posix_timestamp_msec), 'US/Eastern')) AS us_hour,
  CASE EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_MILLIS(H.posix_timestamp_msec), 'US/Eastern'))
    WHEN 1 THEN '1_Sunday'
    WHEN 2 THEN '2_Monday'
    WHEN 3 THEN '3_Tuesday'
    WHEN 4 THEN '4_Wednesday'
    WHEN 5 THEN '5_Thursday'
    WHEN 6 THEN '6_Friday'
    WHEN 7 THEN '7_Saturday'
  END AS us_day_of_week,    
  EXTRACT(MONTH FROM DATETIME(TIMESTAMP_MILLIS(posix_timestamp_msec), 'US/Eastern')) AS us_month,
  EXTRACT(YEAR FROM DATETIME(TIMESTAMP_MILLIS(posix_timestamp_msec), 'US/Eastern')) AS us_year,


  FIRST_VALUE(H.ga_dimension_7 IGNORE NULLS) OVER(session_hits_asc) AS start_page_type,
  H.ga_dimension_80 AS social_network_name,

  NET.REG_DOMAIN(FIRST_VALUE(H.ga_dimension_123) OVER (session_hits_asc)) AS referrer_domain_session,	
  CASE 
    WHEN REGEXP_EXTRACT(FORMAT("%T", NET.HOST(FIRST_VALUE(H.ga_dimension_123) 
      OVER (session_hits_asc))), r'([a-zA-Z.0-9.+-]{1,})') = 'NULL' 
      THEN NULL 
    ELSE REGEXP_EXTRACT(FORMAT("%T", NET.HOST(FIRST_VALUE(H.ga_dimension_123) 
      OVER (session_hits_asc))), r'([a-zA-Z.0-9.+-]{1,})') 
  END AS referrer_hostname_session,

  LOWER(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        CASE
          WHEN (H.ga_dimension_145 like '% %') or (H.ga_dimension_145 = 'none') or (H.ga_dimension_145 is null)
          THEN H.ga_dimension_145
          ELSE `newsuk-datatech-prod.inca_clickstream_views.fn_cmp_consent_lookup`(H.ga_dimension_145)
        END,
        r"^\||\|$", ""
      ),r"\|\|", "|"
    )
  ) AS cmp_consents,

  CASE H.ga_dimension_128 
    WHEN '0' THEN 'navigate'
    WHEN '1' THEN 'reload'
    WHEN '2' THEN 'back_forward'
    WHEN '255' THEN 'other'
  END AS navigation_type,

  CASE 
    WHEN utm_source LIKE 'twitter%' THEN REGEXP_EXTRACT(utm_campaign, r'utm_campaign=([a-zA-Z]{1,})') 
    ELSE NULL 
  END AS twitter_page, 

  REGEXP_EXTRACT(H.ga_dimension_127, '"platform":"(.*?)"') AS experimentation_platform,
  REGEXP_EXTRACT(H.ga_dimension_127, '"uid":"(.*?)"') AS experimentation_user_id,
  ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(REGEXP_EXTRACT(H.ga_dimension_127, '"data":"(.*?)"'),  r'([0-9A-Za-z]+)\|'),',') AS experiment_id_delimited,
  ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(REGEXP_EXTRACT(H.ga_dimension_127, '"data":"(.*?)"'), r'\|([0-9A-Za-z]+)'), ',') AS variant_id_delimited,
  REGEXP_EXTRACT_ALL(regexp_extract(H.ga_dimension_127, '"data":"(.*?)"'), r'([0-9A-Za-z]+)\|') AS experiment_id_nested,
  REGEXP_EXTRACT_ALL(regexp_extract(H.ga_dimension_127, '"data":"(.*?)"'), r'\|([0-9A-Za-z]+)') AS variant_id_nested,

  --Traffic_source framework
  IF(
      REGEXP_CONTAINS(H.url, r'(gaa_(sig|at|n|ts)=)'),
      'aggregator:news_agg:google_showcase',
    `news-data-products-prod.traffic_source.fn_get_traffic_source`(
      `news-data-products-prod.traffic_source.fn_categorise_traffic_by_referrer`(
        LOWER(H.ga_dimension_123), 
        LOWER(net.host(H.ga_dimension_123)), 
        LOWER(net.reg_domain(H.ga_dimension_123))
      ), --AS by_referrer,
      `news-data-products-prod.traffic_source.fn_categorise_traffic_by_utm`(
        -- dropping below until AIT fix utm_metrics fields
          -- LOWER(H.utm_source),
          -- LOWER(H.utm_medium)
          REGEXP_EXTRACT(H.url, r'utm_source=([a-zA-Z0-9_+-]{1,})'),
          REGEXP_EXTRACT(H.url, r'utm_medium=([a-zA-Z0-9_+-]{1,})')
      ), --AS by_utm_param
      `news-data-products-prod.traffic_source.fn_categorise_traffic_by_previous_page`(LOWER(LAG(H.ga_dimension_2, 1) OVER (session_hits_asc))), --AS by_previous_page_name,    
      H.ga_dimension_123,
      LAG(H.ga_dimension_2, 1) OVER (session_hits_asc),
      H.ga_dimension_1
    )
   ) AS traffic_source_hit,
  
  SPLIT(`news-data-products-prod.traffic_source.fn_get_traffic_source`(
    `news-data-products-prod.traffic_source.fn_categorise_traffic_by_referrer`(
      LOWER(H.ga_dimension_123), 
      LOWER(net.host(H.ga_dimension_123)), 
      LOWER(net.reg_domain(H.ga_dimension_123))
    ), --AS by_referrer,
    `news-data-products-prod.traffic_source.fn_categorise_traffic_by_utm`(
      REGEXP_EXTRACT(H.url, r'utm_source=([a-zA-Z0-9_+-]{1,})'),
      REGEXP_EXTRACT(H.url, r'utm_medium=([a-zA-Z0-9_+-]{1,})')
    ), --AS by_utm_param
    `news-data-products-prod.traffic_source.fn_categorise_traffic_by_previous_page`(LOWER(LAG(H.ga_dimension_2, 1) OVER (session_hits_asc))), --AS by_previous_page_name,    
    H.ga_dimension_123,
    LAG(H.ga_dimension_2, 1) OVER (session_hits_asc),
    H.ga_dimension_1
  ), ':')[SAFE_OFFSET(0)] AS traffic_source_group_hit,  
  
  CAST(NULL AS STRING) AS traffic_source_session, --Backfilled on update
  CAST(NULL AS STRING) AS traffic_source_group_session, --Backfilled on update
  CAST(NULL AS STRING) AS traffic_source_type_session, --Backfilled on update

  `news-data-products-prod.traffic_source.fn_get_traffic_source_type`(
    SPLIT(`news-data-products-prod.traffic_source.fn_get_traffic_source`(
      `news-data-products-prod.traffic_source.fn_categorise_traffic_by_referrer`(
        LOWER(H.ga_dimension_123), 
        LOWER(net.host(H.ga_dimension_123)), 
        LOWER(net.reg_domain(H.ga_dimension_123))
      ), --AS by_referrer,
      `news-data-products-prod.traffic_source.fn_categorise_traffic_by_utm`(
        REGEXP_EXTRACT(H.url, r'utm_source=([a-zA-Z0-9_+-]{1,})'),
        REGEXP_EXTRACT(H.url, r'utm_medium=([a-zA-Z0-9_+-]{1,})')
      ), --AS by_utm_param
      `news-data-products-prod.traffic_source.fn_categorise_traffic_by_previous_page`(LOWER(LAG(H.ga_dimension_2, 1) OVER (session_hits_asc))), --AS by_previous_page_name,    
      H.ga_dimension_123,
      LAG(H.ga_dimension_2, 1) OVER (session_hits_asc),
      H.ga_dimension_1
    ), ':')[SAFE_OFFSET(0)], --traffic_source_group
      FIRST_VALUE(
      IF(H.url LIKE "%utm_medium%", REGEXP_EXTRACT(H.url, r"utm_medium=([a-zA-Z0-9_+-]{1,})"), NULL))
      OVER (session_hits_asc), --utm_medium_last_click, 
    `newsuk-datatech-prod.inca_clickstream_views.fn_get_marketing_code`(H.utm_source, H.utm_medium, H.utm_term, H.utm_campaign, H.utm_content), -- marketing_tracking_code,
    H.url -- url
  ) AS traffic_source_type_hit,   
  
  IF(
      `news-data-products-prod.traffic_source.fn_get_traffic_source`(
        `news-data-products-prod.traffic_source.fn_categorise_traffic_by_referrer`(
          LOWER(H.ga_dimension_123), 
          LOWER(net.host(H.ga_dimension_123)), 
          LOWER(net.reg_domain(H.ga_dimension_123))
        ), --AS by_referrer,
        `news-data-products-prod.traffic_source.fn_categorise_traffic_by_utm`(
          REGEXP_EXTRACT(H.url, r'utm_source=([a-zA-Z0-9_+-]{1,})'),
          REGEXP_EXTRACT(H.url, r'utm_medium=([a-zA-Z0-9_+-]{1,})')
        ), --AS by_utm_param
        `news-data-products-prod.traffic_source.fn_categorise_traffic_by_previous_page`(LOWER(LAG(H.ga_dimension_2, 1) OVER (session_hits_asc))), --AS by_previous_page_name,    
        H.ga_dimension_123,
        LAG(H.ga_dimension_2, 1) OVER (session_hits_asc),
        H.ga_dimension_1
      ) LIKE '%google_showcase%',
      'google',
    `news-data-products-prod.traffic_source.fn_get_big_media_owner`(
      `news-data-products-prod.traffic_source.fn_get_traffic_source`(
        `news-data-products-prod.traffic_source.fn_categorise_traffic_by_referrer`(
          LOWER(H.ga_dimension_123), 
          LOWER(net.host(H.ga_dimension_123)), 
          LOWER(net.reg_domain(H.ga_dimension_123))
        ), --AS by_referrer,
        `news-data-products-prod.traffic_source.fn_categorise_traffic_by_utm`(
          REGEXP_EXTRACT(H.url, r'utm_source=([a-zA-Z0-9_+-]{1,})'),
          REGEXP_EXTRACT(H.url, r'utm_medium=([a-zA-Z0-9_+-]{1,})')
        ), --AS by_utm_param
        `news-data-products-prod.traffic_source.fn_categorise_traffic_by_previous_page`(LOWER(LAG(H.ga_dimension_2, 1) OVER (session_hits_asc))), --AS by_previous_page_name,    
        H.ga_dimension_123,
        LAG(H.ga_dimension_2, 1) OVER (session_hits_asc),
        H.ga_dimension_1
      ) 
    )
  ) AS big_media_owner_hit,

  CAST(NULL AS STRING) AS big_media_owner_session, --Backfilled on update
  CASE
     WHEN H.ga_dimension_154 = 'the scottish sun' THEN 'scottish sun'
     WHEN H.ga_dimension_154 = 'the irish sun' THEN 'irish sun'
     ELSE H.ga_dimension_154 
  END AS cms_original_publishing_site,
  H.ga_dimension_15 AS ga_page_additional_1,
  H.ga_dimension_16 AS ga_page_additional_2,
  H.ga_dimension_17 AS ga_page_additional_3,
  H.ga_dimension_18 AS ga_page_additional_4,
  H.ga_dimension_35 AS event_interaction_details_4,

  H.ga_dimension_173 AS palin_question_type,
  H.ga_dimension_5 AS section_level3,

  H.device_screen_resolution,

  H.ga_dimension_146 AS pageview_id,

  H.ga_dimension_130 AS media_id,
  H.ga_dimension_131 AS media_title,
  H.ga_dimension_132 AS media_type,
  H.ga_dimension_133 AS media_player,
  H.ga_dimension_134 AS media_duration,
  H.ga_dimension_135 AS media_offset,
  CAST(H.ga_dimension_136 AS FLOAT64) AS media_milestone,
  H.ga_dimension_137 AS media_segment,

  H.is_interaction_event AS is_interaction_event,
  
  P.product_brand AS product_brand,
  P.product_titlegroup AS product_titlegroup,
  P.is_global_reporting AS is_global_reporting,
  P.is_newsroom_reporting AS is_newsroom_reporting,
  P.dfp_adunit1 AS dfp_adunit1,
  P.dfp_os AS dfp_os,
  P.display_name AS display_name,
  P.product_region AS product_region,

  H.ga_dimension_148 AS synced_visitor_id,

  ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(REGEXP_EXTRACT(H.ga_dimension_129, '"key":"(.?)"'),  r'([0-9A-Za-z_-]+):'),',') AS full_stack_experiment_id_delimited,
  ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(REGEXP_EXTRACT(H.ga_dimension_129, '"key":"(.?)"'), r':([0-9A-Za-z_-]+)'), ',') AS full_stack_variant_id_delimited,

  H.ga_dimension_20 AS splash_teaser_name,
  COUNTIF(H.is_interaction_event = 1) OVER(session_hits_asc) AS session_interaction_hit_count


FROM 
  `newsuk-datatech-prod.inca_clickstream_views.sun_ga_base_view` H
  LEFT JOIN `newsuk-datatech-prod.inca_clickstream_tables.ref_product` P ON H.ga_dimension_1 = P.product AND P.clickstream_source = "sun_clickstream"
  LEFT JOIN `newsuk-datatech-prod.inca_clickstream_tables.ref_geo_country` C ON LOWER(H.geo_country) = LOWER(C.countryname)  
  LEFT JOIN `newsuk-datatech-prod.inca_clickstream_tables.ref_ga_region_code` R ON LOWER(H.geo_region) = LOWER(R.region)
  LEFT JOIN `newsuk-datatech-prod.inca_clickstream_tables.lkp_referrer_type_sun` RT ON LOWER(NET.REG_DOMAIN(H.ga_dimension_123)) = LOWER(RT.domain_name)
  
WINDOW
  session_hits_asc AS (PARTITION BY table_suffix, session_id ORDER BY posix_timestamp_msec),
  visit_hits_asc AS (PARTITION BY table_suffix, visitor_id ORDER BY posix_timestamp_msec)
