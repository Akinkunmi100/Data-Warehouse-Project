
    
    

select
    submission_uuid as unique_field,
    count(*) as n_records

from "warehouse"."client_mtn"."project_appraise"
where submission_uuid is not null
group by submission_uuid
having count(*) > 1


