SELECT
  activity_date_time,
  activity_date,
  activity_hour,
  EXTRACT(month FROM activity_date) AS month,
  CASE EXTRACT(DAYOFWEEK FROM activity_date)
    WHEN 1 THEN '1_Sunday'
    WHEN 2 THEN '2_Monday'
    WHEN 3 THEN '3_Tuesday'
    WHEN 4 THEN '4_Wednesday'
    WHEN 5 THEN '5_Thursday'
    WHEN 6 THEN '6_Friday'
    WHEN 7 THEN '7_Saturday'
  END AS day_of_week,
  CS.content_cms_id,
  session_id,
  visitor_id,
  is_page_view, --can deprecate
  CS.content_id,
  url,
  page_name,
  CASE
    WHEN product = 'the sun' THEN 'web'
    WHEN product = 'the sun amp' THEN 'amp'
    WHEN product = 'the sun mobile ios app 2017' THEN 'phone app'
    WHEN product = 'the sun mobile android app 2017' THEN 'phone app'
    WHEN product = 'thesun facebook instant articles' THEN 'fbia'
    WHEN product = 'scottish sun' THEN 'web'
    WHEN product = 'irish sun' THEN 'web'
    WHEN product = 'scottish sun amp' THEN 'amp'
    WHEN product = 'irish sun amp' THEN 'amp'
    WHEN product = 'the sun google news' THEN 'google news'
    WHEN product = 'the sun google news' THEN 'google news'
    WHEN product = 'the sun mobile ios app' THEN 'phone app'
    WHEN product = 'the sun us' THEN 'web'
    WHEN product = 'the sun us amp' THEN 'amp'
    WHEN product = 'the sun us facebook instant articles' THEN 'fbia'
    when product = 'thesun facebook instant articles scotland' then 'fbia'
  END AS platform,
 CASE 
    WHEN product = 'the sun' THEN 'the sun'
    WHEN product = 'the sun amp' THEN 'the sun'
    WHEN product = 'the sun mobile ios app 2017' THEN 'the sun'
    WHEN product = 'the sun mobile android app 2017' THEN 'the sun'
    WHEN product = 'thesun facebook instant articles' THEN 'the sun'
    WHEN product = 'scottish sun' THEN 'scottish sun'
    WHEN product = 'irish sun' THEN 'irish sun'
    WHEN product = 'scottish sun amp' THEN 'scottish sun'
    WHEN product = 'irish sun amp' THEN 'irish sun'
    WHEN product = 'the sun google news' THEN 'the sun'
    WHEN product = 'the sun mobile ios app' THEN 'the sun'
    WHEN product = 'the sun us' THEN 'the us sun'
    WHEN product = 'the sun us amp' THEN 'the us sun'
    WHEN product = 'the sun us facebook instant articles' THEN 'the us sun'
    WHEN product = 'the sun us google news' THEN 'the us sun'
    WHEN product = 'thesun facebook instant articles scotland' THEN 'scottish sun'
  END AS brand,
  
 CASE LOWER(country_name)
    WHEN 'united kingdom' THEN 'UK'
    WHEN 'united states' THEN 'US'
    WHEN 'ireland' THEN 'ROI'
    Else 'ROW' end as country_grouping,
    
 IF(LOWER(country_name) = 'united kingdom', 'UK','Global') as geo_grouping,
    
 CASE
    WHEN customer_type = 'guest' THEN 'anonymous'
    WHEN customer_type = 'registered' THEN 'registered'
    WHEN customer_type is null then  'anonymous'
    end registered_status,
    
 CASE
    WHEN operating_system = 'iOS' THEN 'iOS'
    WHEN operating_system = 'Android' THEN 'Android'
    WHEN operating_system = 'Windows' then  'Windows'
    WHEN operating_system = 'Macintosh' then  'MacOS'
    Else 'Other'
    end operating_system_simple,
    
 CASE
    WHEN product = 'the sun' THEN 'national'
    WHEN product = 'the sun amp' THEN 'national'
    WHEN product = 'the sun mobile ios app 2017' THEN 'national'
    WHEN product = 'the sun mobile android app 2017' THEN 'national'
    WHEN product = 'thesun facebook instant articles' THEN 'national'
    WHEN product = 'scottish sun' THEN 'regional'
    WHEN product = 'irish sun' THEN 'regional'
    WHEN product = 'scottish sun amp' THEN 'regional'
    WHEN product = 'irish sun amp' THEN 'regional'
    WHEN product = 'the sun google news' THEN 'national'
    WHEN product = 'the sun mobile ios app' THEN 'national'
    WHEN product = 'the sun us' THEN 'regional'
    WHEN product = 'the sun us amp' THEN 'regional'
    WHEN product = 'the sun us google news' THEN 'regional'
    WHEN product = 'the sun us facebook instant articles' THEN 'regional'
    when product = 'thesun facebook instant articles scotland' then 'regional'
    End national_regional,   
    
  CASE
    WHEN product = 'the sun' THEN 'false'
    WHEN product = 'the sun amp' THEN 'true'
    WHEN product = 'the sun mobile ios app 2017' THEN 'false'                
    WHEN product = 'the sun mobile android app 2017' THEN 'false'
    WHEN product = 'thesun facebook instant articles' THEN 'true'
    WHEN product = 'scottish sun' THEN 'false'
    WHEN product = 'irish sun' THEN 'false'
    WHEN product = 'scottish sun amp' THEN 'true'
    WHEN product = 'irish sun amp' THEN 'true'
    WHEN product = 'the sun google news' THEN 'true'
    WHEN product = 'the sun mobile ios app' THEN 'false'
    WHEN product = 'the sun us' THEN 'false'
    WHEN product = 'the sun us amp' THEN 'true'
    WHEN product = 'the sun us google news' THEN 'true'
    WHEN product = 'the sun us facebook instant articles' THEN 'true'
    when product = 'thesun facebook instant articles scotland' then 'true'
    End is_off_platform,
    
    --Is this still needed?
    FIRST_VALUE(
      CASE 
        WHEN (url like "%utm_medium=email%" OR url like "%?cmp=emc%") --AND FIRST_VALUE(is_page_view) OVER (PARTITION BY session_id, activity_date ORDER BY session_hit_count) = 1  
        then "Email" 
        WHEN  (url like "%utm_medium=display%" OR url like '%?cmp=bac%')  --AND FIRST_VALUE(is_page_view) OVER (PARTITION BY session_id, activity_date ORDER BY session_hit_count) = 1 
        then "Display" 
        WHEN (url like "%paid_social%" OR url like "%paidsocial%" OR url like "%vccp%" OR url like "%?cmp%boost%") --AND FIRST_VALUE(is_page_view) OVER (PARTITION BY session_id, activity_date ORDER BY session_hit_count) = 1 
        then "Paid Social" 
        WHEN (url like "%utm_medium=paid_search%" OR url like "%?cmp=KN_%") --AND FIRST_VALUE(is_page_view) OVER (PARTITION BY session_id, activity_date ORDER BY session_hit_count) = 1 
        then "Paid Search" 
        WHEN product in ('thesun facebook instant articles', 'thesun facebook instant articles scotland',
                          'the sun us facebook instant articles') then "Organic Social"
        WHEN product IN ('the sun amp', 'scottish sun amp', 'irish sun amp') THEN "Organic Search"
        WHEN nr_domain_type like "Search Engines" THEN "Organic Search"
        WHEN nr_domain_type like "Social Networks" THEN "Organic Social"
        WHEN nr_domain_type like "Referral" then "Referral" 
        WHEN referrer_hit is null then "No Referrer"  
        ELSE "Referral"
      END
     ) OVER (session_hit_asc) AS newsroom_referrer_type_session,
    
    case 
      when net.reg_domain(referrer_session) is null then net.host(referrer_session) 
      else net.reg_domain(referrer_session)
    end as referring_domain,
    
 CASE
    WHEN page_type = 'article' THEN 'article'
    WHEN page_type = 'homepage' THEN 'homepage'
    WHEN page_type = 'latest' THEN 'homepage'
    WHEN page_type = 'image' THEN 'image / video'
    WHEN page_type = 'section level 1' THEN 'index'
    WHEN page_type = 'picture' THEN 'image / video'
    WHEN page_type = 'section level 2' THEN 'index'
    WHEN page_type = 'topic' THEN 'index'
    WHEN page_type = 'video' THEN 'image / video'
    WHEN page_type = 'errors' THEN 'other'
    WHEN page_type = 'team page' THEN 'other'
    WHEN page_type = 'search' THEN 'other'
    WHEN page_type = 'people' THEN 'other'
    WHEN page_type = 'topics selection' THEN 'other'
    WHEN page_type = 'section level 3' THEN 'index'
    WHEN page_type = 'login' THEN 'other'
    WHEN page_type = 'registration form' THEN 'other'
    WHEN page_type = 'login form' THEN 'other'
    WHEN page_type = 'index' THEN 'other'
    WHEN page_type is null THEN 'other'
    Else 'other'
    end page_type_group_level_1,
    
 CASE
    WHEN page_type = 'article' THEN 'article'
    WHEN page_type = 'homepage' THEN 'homepage'
    WHEN page_type = 'latest' THEN 'mobile app homepage'
    WHEN page_type = 'image' THEN 'image'
    WHEN page_type = 'section level 1' THEN 'section level 1 index'
    WHEN page_type = 'picture' THEN 'image'
    WHEN page_type = 'section level 2' THEN 'section level 2 index'
    WHEN page_type = 'topic' THEN 'topic index'
    WHEN page_type = 'video' THEN 'video'
    WHEN page_type = 'errors' THEN 'other'
    WHEN page_type = 'team page' THEN 'other'
    WHEN page_type = 'search' THEN 'other'
    WHEN page_type = 'people' THEN 'other'
    WHEN page_type = 'topics selection' THEN 'other'
    WHEN page_type = 'section level 3' THEN 'section level 3 index'
    WHEN page_type = 'login' THEN 'other'
    WHEN page_type = 'registration form' THEN 'other'
    WHEN page_type = 'login form' THEN 'other'
    WHEN page_type = 'index' THEN 'other'
    WHEN page_type is null THEN 'other'
    Else 'other'
    end page_type_group_level_2,
    
    CASE
    WHEN lower(trim(section)) = 'image galleries' THEN 'image galleries'  
    WHEN lower(trim(section)) = 'news' THEN 'news'
    WHEN lower(trim(section)) = 'tv & showbiz' and activity_date < '2020-07-17' THEN 'tv & showbiz'
    WHEN lower(trim(section)) = 'tv &amp; showbiz' and activity_date < '2020-07-17' THEN 'tv & showbiz'
    WHEN lower(trim(section)) = 'tv' and activity_date < '2020-07-17' THEN 'tv & showbiz'
    WHEN lower(trim(section)) = 'showbiz' and activity_date < '2020-07-17' THEN 'tv & showbiz'
    WHEN lower(trim(section)) = 'tv & showbiz' and activity_date >= '2020-07-17' THEN 'showbiz'
    WHEN lower(trim(section)) = 'tv &amp; showbiz' and activity_date >= '2020-07-17' THEN 'showbiz'
    WHEN lower(trim(section)) = 'tv' and activity_date >= '2020-07-17' THEN 'tv'
    WHEN lower(trim(section)) = 'showbiz' and activity_date >= '2020-07-17' THEN 'showbiz'
    WHEN lower(trim(section)) = 'sport' THEN 'sport'
    WHEN lower(trim(section)) = 'homepage' THEN 'homepage'
    WHEN lower(trim(section)) = 'fabulous' THEN 'fabulous'
    WHEN lower(trim(section)) = 'living' THEN 'living'
    WHEN lower(trim(section)) = 'money' THEN 'money'
    WHEN lower(trim(section)) = 'tech' THEN 'tech'
    WHEN lower(trim(section)) = 'image galleries' THEN 'image galleries'
    WHEN lower(trim(section)) = 'dear deidre' THEN 'dear deidre'
    WHEN lower(trim(section)) = 'travel' THEN 'travel'
    WHEN lower(trim(section)) = 'world cup 2018' THEN 'world cup 2018'
    WHEN lower(trim(section)) = 'motors' THEN 'motors'
    WHEN lower(trim(section)) = 'topics' THEN 'topics'
    WHEN lower(trim(section)) = 'page3' THEN 'page3'
    WHEN lower(trim(section)) = 'errors' THEN 'errors'
    WHEN lower(trim(section)) = 'entertainment' THEN 'entertainment'
    WHEN lower(trim(section)) = 'lifestyle' THEN 'lifestyle'
    WHEN lower(trim(section)) = 'health' THEN 'health'
    Else 'other'
    END AS section_simple,

    section,
    SPLIT(lower(section_level2), ':')[SAFE_OFFSET(1)]  as section_level_2,
    device_type,
    facebook_page,
    facebook_post_date,    
    
    extract(year from activity_date) as activity_year,
    SAFE_CAST(user_local_time AS DATETIME) AS user_local_time,
    us_date_time,
    us_date,
    us_hour,
    us_day_of_week,
    us_month,
    us_year,
    session_hit_count,    
    
    FIRST_VALUE(
      CASE
        WHEN page_type = 'article' THEN 'article'
        WHEN page_type IN ('homepage', 'latest') THEN 'homepage'
        WHEN page_type IN ('image', 'picture', 'video') THEN 'image / video'
        WHEN page_type IN ('section level 1', 'section level 2', 'topic') THEN 'index'
        ELSE 'other'
      END
    ) OVER (session_asc) AS start_page_type,
    author_name AS cms_author_name,
    headline AS cms_headline,
    cms.content_cms_id AS cms_content_cms_id,
    newsroom_cms_topic_tags_v1 AS cms_topic_tags,
    cms_last_modified,
    cms_first_published,
    SPLIT(newsroom_cms_topic_tags_v1, ',') AS cms_topic_tags_nested,
    cms_section,
    cms_sub_section,
    
    twitter_page,
    `newsuk-datatech-prod.inca_clickstream_tables.fn_get_page_referrer_source`(traffic_source_hit,traffic_source_group_hit) AS page_referrer_source,
    `newsuk-datatech-prod.inca_clickstream_tables.fn_get_page_referrer_type`(traffic_source_group_hit, traffic_source_type_hit, traffic_source_hit ) AS page_referrer_type, 
    `newsuk-datatech-prod.inca_clickstream_tables.fn_get_session_referrer_source`(traffic_source_session,traffic_source_group_session) AS session_referrer_source,
    `newsuk-datatech-prod.inca_clickstream_tables.fn_get_session_referrer_type`(traffic_source_group_session, traffic_source_type_session,  traffic_source_session) AS session_referrer_type,
    big_media_owner_hit as page_referrer_grouped,
    big_media_owner_session as session_referrer_grouped,
    cms_original_publishing_site,
    country_name AS country,
    DATE_TRUNC(activity_date, WEEK(MONDAY)) AS activity_week_start_date,
    DATE_TRUNC(us_date, WEEK(MONDAY)) AS us_week_start_date,
    
    cms_first_published_week_start_date,
    traffic_source_type_hit as page_referrer_paid_organic,
    traffic_source_type_session as session_referrer_paid_organic,
    CASE 
      WHEN `newsuk-datatech-prod.inca_clickstream_tables.fn_get_session_referrer_type`(traffic_source_group_session, traffic_source_type_session,  traffic_source_session) = 'direct' AND FIRST_VALUE(page_type) OVER(session_asc) IN ('homepage', 'latest') THEN 'true direct'
      WHEN `newsuk-datatech-prod.inca_clickstream_tables.fn_get_session_referrer_type`(traffic_source_group_session, traffic_source_type_session,  traffic_source_session) = 'direct' AND FIRST_VALUE(page_type) OVER(session_asc) IN ('section level 1', 'section level 2', 'topic') THEN 'true direct'
      WHEN `newsuk-datatech-prod.inca_clickstream_tables.fn_get_session_referrer_type`(traffic_source_group_session, traffic_source_type_session,  traffic_source_session) = 'search' AND FIRST_VALUE(page_type) OVER(session_asc) IN ('homepage', 'latest') THEN 'true direct'
      ELSE 'other' 
    END AS session_referrer_true_direct,
    LAG(CS.content_id) OVER(session_hit_asc) AS previous_hit_content_id,
    LAG(page_name) OVER(session_hit_asc) AS previous_hit_page_name,
    LAG(section) OVER(session_hit_asc) AS previous_hit_section,

    CMS.isOriginatedFrom AS origin_url,
    REGEXP_EXTRACT(CMS.isOriginatedFrom, r'^(?:https?:\/\/)?(?:[^@\n]+@)?(?:www\.)?([^:\/\n?]+)') AS origin_brand
    
FROM 
   `newsuk-datatech-prod.inca_clickstream_tables.sun_clickstream_daily` CS
  LEFT JOIN `newsuk-datatech-prod.inca_clickstream_tables.lkp_referrer_type_sun` RSES ON LOWER(NET.REG_DOMAIN(referrer_session)) = LOWER(domain_name)
  LEFT JOIN `news-data-products-prod.av_content.ngn_cms_content_current` CMS ON CS.content_cms_id = CMS.content_cms_id
WHERE 
  is_page_view = 1
  AND product in (
    'the sun',
    'the sun amp',
    'the sun mobile ios app 2017',
    'the sun mobile android app 2017',
    'thesun facebook instant articles',
    'scottish sun',
    'irish sun',
    'scottish sun amp',
    'irish sun amp',
    'the sun google news',
    'the sun us google news',
    'the sun mobile ios app',
    'the sun us',
    'the sun us amp',
    'the sun us facebook instant articles',
    'thesun facebook instant articles scotland'
   )
WINDOW
  session_asc AS (PARTITION BY session_id, activity_date ORDER BY activity_date_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
  session_hit_asc AS (PARTITION BY session_id, activity_date ORDER BY session_hit_count)
