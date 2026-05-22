
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select submission_uuid
from "warehouse"."client_mtn"."project_appraise"
where submission_uuid is null



  
  
      
    ) dbt_internal_test