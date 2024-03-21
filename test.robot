*** Settings ***
Resource  Keywords.robot
Test Setup  Connect To SQLite Database
Test Teardown  Disconnect From Database

*** Variables ***
${TABLE}  COMPANY
${TABLE_ANSI}  COMPANY_ANSI


*** Test Cases ***
Verify table on source contains address California
    # Ideal case to get exactly data from DB
    ${query_results}  Query  SELECT * FROM ${TABLE}  returnAsDict=${TRUE}  alias=source
    Perform check that address California is contained in response  ${query_results}
    ${query_results}  Query  SELECT * FROM ${TABLE} WHERE ADDRESS='California'  returnAsDict=${TRUE}  alias=source
    Perform check that address California is contained in response  ${query_results}

Verify table contains same data between source and target
    ${source_tbl}  Get all data from table  ${TABLE}  source
    ${target_tbl}  Get all data from table  ${TABLE}  target
    Sort List  ${source_tbl}
    Sort List  ${target_tbl}
    Perform data comparison check of tables  ${source_tbl}  ${target_tbl}

Perform row count check of tables
    ${cnt_source}  Get row count  ${TABLE}  source
    ${cnt_target}  Get row count  ${TABLE}  target
    Perform row count check of tables  ${cnt_source}  ${cnt_target}

Verify primary keys are unique
    ${prim_keys_cnt}  Get primary keys count  ${TABLE}  name  target
    Perform primary key uniqueness check  ${prim_keys_cnt}

Verify sample data (ANSI)
    ${cnt}  Get row count  ${TABLE}  source
    ${samples_s}  Get random sample from table  ${TABLE}  ${50}  ${cnt}
    Log many  Only this samples from source will be compared against source  ${samples_s}  Row count is ${cnt}
    ${cols_source}  Get table column names  ${TABLE}  source
    ${cols_target}  Get table column names  ${TABLE_ANSI}  target
    ${samples_t}  Get samples from target for comparison  ${samples_s}  ${cols_source}  ${cols_target}
    ${igored_s_index}  ${igored_t_index}  Get ignored column index source and target  ${cols_source}  ${cols_target}
    Perform data comparison check of tables  ${samples_s}  ${samples_t}  ${igored_s_index}





