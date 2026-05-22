
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    submission_uuid as unique_field,
    count(*) as n_records

from "warehouse"."client_mtn"."project_appraise"
where submission_uuid is not null
group by submission_uuid
having count(*) > 1



  
  
      
    ) dbt_internal_test