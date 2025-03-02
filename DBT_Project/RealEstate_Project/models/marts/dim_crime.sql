with 
    incident as (
        select
            *
        from {{ ref('stg_crime_incident') }}
    )
    ,agency as (
        select
            *
        from {{ ref('stg_crime_agencies') }}
    )
    ,offense as (
        select
            *
        from {{ source('realestate', 'CRIME_OFFENSE') }}
    )
    ,offense_type as (
        select
            *
        from {{ source('realestate', 'CRIME_OFFENSE_TYPE') }}
    )
    ,dim_crime as (
        select ci.incident_id, ci.data_year as YEAR, ca.county_name, co.offense_id, cot.offense_name,cot.offense_category_name,dc.county_id
        from incident as ci  
        join agency as ca on ci.agency_id = ca.agency_id
        join offense as co on ci.incident_id = co.incident_id
        join offense_type as cot on co.offense_code = cot.offense_code
        join {{ source('realestate', 'DIM_COUNTY') }} as dc on ca.county_name = dc.county_name
    )
select * from dim_crime





