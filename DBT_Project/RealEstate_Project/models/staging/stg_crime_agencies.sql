with
    source as (
        select
            *
        from {{ source('realestate', 'RAW_CRIME_AGENCIES') }}
        where county_name like '%DALLAS%'
        OR county_name like '%DENTON%'
        OR county_name like '%COLLIN%'
    )

    select
        YEARLY_AGENCY_ID,
        AGENCY_ID  ,
        DATA_YEAR  ,
        ORI  ,
        LEGACY_ORI  ,
        COVERED_BY_LEGACY_ORI  ,
        DIRECT_CONTRIBUTOR_FLAG  ,
        DORMANT_FLAG  ,
        DORMANT_YEAR  ,
        REPORTING_TYPE  ,
        UCR_AGENCY_NAME  ,
        NCIC_AGENCY_NAME  ,
        PUB_AGENCY_NAME  ,
        PUB_AGENCY_UNIT  ,
        AGENCY_STATUS  ,
        STATE_ID  ,
        STATE_NAME  ,
        STATE_ABBR  ,
        STATE_POSTAL_ABBR  ,
        DIVISION_CODE  ,
        DIVISION_NAME  ,
        REGION_CODE  ,
        REGION_NAME  ,
        REGION_DESC  ,
        AGENCY_TYPE_NAME  ,
        POPULATION  ,
        SUBMITTING_AGENCY_ID  ,
        SAI  ,
        SUBMITTING_AGENCY_NAME  ,
        SUBURBAN_AREA_FLAG  ,
        POPULATION_GROUP_ID  ,
        POPULATION_GROUP_CODE  ,
        POPULATION_GROUP_DESC  ,
        PARENT_POP_GROUP_CODE  ,
        PARENT_POP_GROUP_DESC  ,
        MIP_FLAG  ,
        POP_SORT_ORDER  ,
        SUMMARY_RAPE_DEF  ,
        PE_REPORTED_FLAG  ,
        MALE_OFFICER  ,
        MALE_CIVILIAN  ,
        MALE_OFFICER_MALE_CIVILIAN  ,
        FEMALE_OFFICER  ,
        FEMALE_CIVILIAN  ,
        FEMALE_OFFICER_FEMALE_CIVILIAN  ,
        OFFICER_RATE ,
        EMPLOYEE_RATE ,
        NIBRS_CERT_DATE ,
        NIBRS_START_DATE ,
        NIBRS_LEOKA_START_DATE ,
        NIBRS_CT_START_DATE ,
        NIBRS_MULTI_BIAS_START_DATE ,
        NIBRS_OFF_ETH_START_DATE ,
        COVERED_FLAG  ,
        CASE 
            WHEN county_name LIKE '%DALLAS%' THEN 'DALLAS'
            WHEN county_name LIKE '%DENTON%' THEN 'DENTON'
            WHEN county_name LIKE '%COLLIN%' THEN 'COLLIN'
        end as COUNTY_NAME, 
        MSA_NAME  ,
        PUBLISHABLE_FLAG  ,
        PARTICIPATED  ,
        NIBRS_PARTICIPATED  
    from source