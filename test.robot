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
    ${source_tbl}  Get all data from table  table=${TABLE}  origin=source
    ${target_tbl}  Get all data from table  table=${TABLE}  origin=target
    Sort List  ${source_tbl}
    Sort List  ${target_tbl}
    Perform data comparison check of tables  table1=${source_tbl}  table2=${target_tbl}

Verify row count check of tables
    ${cnt_source}  Get row count  table=${TABLE}  origin=source
    ${cnt_target}  Get row count  table=${TABLE}  origin=target
    Perform row count check of tables  count1=${cnt_source}  count2=${cnt_target}

Verify primary keys are unique
    ${prim_keys_cnt}  Get primary keys count  table=${TABLE}  col_name=name  origin=target
    Perform primary key uniqueness check  ${prim_keys_cnt}

Verify sample data (ANSI)
    ${cnt}  Get row count  table=${TABLE}  origin=source
    ${samples_s}  Get random sample from table  table=${TABLE}  percentage=${50}  row_cnt=${cnt}
    Log many  Only this samples from source will be compared against source  ${samples_s}  Whereas row count is ${cnt}
    ${cols_source}  Get table column names  table=${TABLE}  origin=source
    ${cols_target}  Get table column names  table=${TABLE_ANSI}  origin=target
    ${samples_t}  Get samples from target for comparison  samples_s=${samples_s}  cols_source=${cols_source}  cols_target=${cols_target}
    ${igored_s_index}  ${igored_t_index}  Get ignored column index source and target  source=${cols_source}  target=${cols_target}
    Perform data comparison check of tables  table1=${samples_s}  table2=${samples_t}  ignore_index_s=${igored_s_index}  ignore_index_t=${igored_t_index}





