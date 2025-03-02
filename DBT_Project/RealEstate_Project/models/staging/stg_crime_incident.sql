with
    source as (
        select
            *
        from {{ source('realestate', 'RAW_CRIME_INCIDENT') }}
    )

select *
from source