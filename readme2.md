### PRODUCT-METRICS-DSS-DE

```bash
├── flows
| ├──project
|   ├── master_recon_check_flow
|   |   |   ├── ddl
|   |   |   |   ├── *.sql
|   |   |   ├── sql
|   |   |   |   ├── *.sql
|   |   |   ├── flow.py or flow_*.py
|   ├── mixed_cube_master
|   |   |   ├── ddl
|   |   |   |   ├── *.sql
|   |   |   ├── sql
|   |   |   |   ├── *.sql
|   |   |   ├── flow.py or flow_*.py
├── Dockerfile
├── Jenkinsfile
└── global_config.yml
```

Each flow will have its own folder, the flow.py (or a file matching the pattern flow_*.py) will define the actual flow that gets registered into Prefect. Please note that only 1 flow.py or flow_*.py file can be defined per directory.  Multiple flow python files will cause the Jenkins CI/CD prefect registration to fail.  You can define variables in your global_config.yml (which is available to all flows) or you can keep it local to your flow in the flow_config.yml.  The readme.md follows the same concept.

The only difference between the master_recon_check_flow and mixed_cube_master is that the mixed_cube_master does not perform reconciliation checks.  The DDL and SQL directories under each respective folder should match.  The tasks in each flow (execute sql tasks (SFSQLFile)) and order of execution should match as well.

Lastly, a recon_test flow was created for testing/POC purposes.  This matches the master_recon_check_flow but does not execute any stored procedures.  Rather, it only executes the reconcilation checks.

After UAT, the recon_test and mixed_cube_master flows can be deleted.

## Jenkins Jobs for this Repo
### NONPROD (NOTE:  SPs are only defined to Prod Snowflake)
DEV/QA:
https://jenkinsx.pgraws98.com/job/Product_Metrics_DSS_DE_FLOW/

### PRODUCTION
PROD (deploys MAIN branch):
https://jenkinsp.pgraws98.com/job/Product_Metrics_DSS_DE_FLOW/

## Prefect Project (Team = Product, Projects = RTR-EZL-ENV (i.e., RTR-EZL-DEV))
https://cloud.prefect.io/pgr-product

## Snowflake Shell
https://github.com/PCDST/snowflake-shell-configs-product

## Ingestion Process
### Infrastructure
### master_recon_check_flow
This flow is (currently) manually run on the Sunday after month end processing.  The first tasks in the flow execute queries to validate that a reconciliation record occurs in the customerhub_profiles.control.reconciliation_log/counts tables (for customerhub data only) and dss_operational_data.data_pipeline_reconciliation.reconciliation_log/counts tables (for all other tables).  If no (0) rows exist for a given process_date, the flow will fail and return an error (a message will be returned to the prefect execution log).

The tables are supplied in the master_recon_check_flow/sql/ directory in text files.  They are broken up by database and process date.

The following is a listing of which process dates are utilized for which tables.

|Database               |Process_Date                           |
|-----------------------|---------------------------------------|
|mdw_copy               |last calendar day of prior month       |
|plr_copy               |current date                           |
|plr_copy               |last calendar day of prior month       |
|sls_copy               |last calendar day of prior month       |
|adw_copy               |prior day                              |
|common_dimension       |prior day                              |
|telematics             |prior day                              |
|customerhub_profiles   |prior day                              |
|dca_copy               |prior day                              |

If rows exist in the reconciliation tables, then the flow will proceed and execution the stored procedures in sequential order, as defined by the flow.

The last SQL query that the flow executes will insert a row into the pl_product_metrics.target.process_status table.  This can be used for downstream dependency checking (namely SSIS) to ensure that all of the stored procedures have completed prior to building any dependent SSAS cubes.


### Manual Reconciliation Checking
To assist in reconciliation checking, the following query was created.  This relies upon the PL_PRODUCT_METRICS.TARGET.RECON_STATUS to be populated will dependent database name, table name, and frequency.  If dependent tables need to be added, the RECON_STATUS table will need to be updated as well.

The output of the query will contain any database table that the flow is dependent upon that is currently out of date.

Query:  
--RECON check with CTEs  
  
with recon_cte as (  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;select  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.db_name,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.tb_name,   
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;max(l.process_date_time) as max_process_date_time,   
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.freq  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;from   
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;PL_PRODUCT_METRICS.TARGET.RECON_STATUS r    
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;left join dss_operational_data.data_pipeline_reconciliation.reconciliation_log l  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;on upper(l.reconciliation_database) = upper(r.db_name)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;and upper(l.reconciliation_name) = upper(r.tb_name)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;left join dss_operational_data.data_pipeline_reconciliation.reconciliation_counts c  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;on l.reconciliation_id = c.reconciliation_id  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;where  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;c.row_count_type = 'final - target'  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;--needed to add the code below due to plr_copy.pol_dates_sl having process date='2022-12-31'  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;and l.process_date_time <= current_date()  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;group by  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.db_name,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.tb_name,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.freq  
UNION  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;select  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.db_name,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.tb_name,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;max(l.process_date) as max_process_date_time,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.freq  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;from  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;PL_PRODUCT_METRICS.TARGET.RECON_STATUS r   
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;left join customerhub_profiles.control.reconciliation_log l  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;on upper(l.reconciliation_name) = upper(r.tb_name)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;left join customerhub_profiles.control.reconciliation_counts c  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;on l.reconciliation_id = c.reconciliation_id  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;where  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;c.reconciliation_type = 'final'  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;group by  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.db_name,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.tb_name,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;r.freq  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;),  
    check_cte as(  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;select  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;db_name,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;tb_name,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;max_process_date_time,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;case  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;when freq = 'CURRENT_DATE()' then CURRENT_DATE()  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;when freq = 'CURRENT_DATE()-1' then CURRENT_DATE() -1  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;when freq = 'last_day(dateadd(''month'',-1,current_date))' then last_day(dateadd('month', -1, current_date))  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;end as expected_date  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;from  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;recon_cte  
    )  

select  
    *  
from
    check_cte  
where
    expected_date <> max_process_date_time order by 1, 2;  




## How to contribute
Machine Setup (DAESETUP for Windows) - https://githubprod.prci.com/progressive/dae_setup#requirements-and-installation-of-dae_setup
