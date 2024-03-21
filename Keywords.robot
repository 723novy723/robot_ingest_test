*** Settings ***
Library  DatabaseLibrary
Library  Collections

*** Variables ***
${DB_PATH_SOURCE}  ${EXECDIR}${/}DB${/}pythonsqlite_source
${DB_PATH_TARGET}  ${EXECDIR}${/}DB${/}pythonsqlite_target

*** Keywords ***
Connect to sqlite database
    Connect To Database Using Custom Params  sqlite3  '${DB_PATH_SOURCE}'  alias=source
    Connect To Database Using Custom Params  sqlite3  '${DB_PATH_TARGET}'  alias=target

Disconnect from db
    Disconnect From All Databases

Perform row count check of tables
    [Arguments]  ${count1}  ${count2}
    Should Be Equal As Integers  ${count1}  ${count2}

Perform primary key uniqueness check
    [Arguments]  ${primary_keys_cnt}
    Length should be  ${primary_keys_cnt}  0

Give number of required samples
    [Arguments]  ${cnt}  ${percentage}
    ${sample_size}  Evaluate  math.ceil(${cnt} * ${percentage} / ${100})
    [Return]  ${sample_size}

Distribute cumulative
    [Arguments]  ${percentage}  ${number}
    ${decimal}  Evaluate  ${percentage} / 100.0
    @{distribution}  Create List
    ${range_limit}  Evaluate  int(1 / ${decimal})
    FOR  ${i}  IN RANGE  ${range_limit}
        ${value}  Evaluate  math.ceil(${decimal} * ${number} * ${i})
        Append To List  ${distribution}  ${value}
    END
    [Return]  ${distribution}

Perform data comparison check of tables
    [Arguments]  ${table1}  ${table2}  ${ignore_index_s}=${EMPTY}  ${ignore_index_t}=${EMPTY}
    ${table1_rows}  Get Length  ${table1}
    ${table2_rows}  Get Length  ${table2}
    Should Be Equal As Integers  ${table1_rows}  ${table2_rows}
    FOR  ${i}  IN RANGE  ${table1_rows}
        ${table1_row}  Get From List  ${table1}  ${i}
        ${table2_row}  Get From List  ${table2}  ${i}
        Compare Rows  ${table1_row}  ${table2_row}  ${ignore_index_s}  ${ignore_index_t}
    END

Compare Rows
    [Arguments]  ${row1}  ${row2}  ${ignore_index_s}  ${ignore_index_t}
    ${row1_columns}  Get Length  ${row1}
    ${row2_columns}  Get Length  ${row2}
    IF  '${ignore_index_s}'!='${EMPTY}'
        ${row1}  Evaluate  tuple([x for i, x in enumerate(${row1}) if i not in ${ignore_index_s}])
        ${row1_columns}  Get Length  ${row1}
    ELSE IF  '${ignore_index_t}'!='${EMPTY}'
         ${row2}  Evaluate  tuple([x for i, x in enumerate(${row2}) if i not in ${ignore_index_t}])
         ${row2_columns}  Get Length  ${row2}
    ELSE
        Should Be Equal As Integers  ${row1_columns}  ${row2_columns}
    END
    Log Many  ${row1}  ${row2}
    FOR  ${i}  IN RANGE  ${row1_columns}
        ${cell1}  Get From List  ${row1}  ${i}
        ${cell2}  Get From List  ${row2}  ${i}
        Run Keyword And Continue On Failure  Should Be Equal  ${cell1}  ${cell2}
    END

Perform check that address California is contained in response
    [Arguments]  ${query_result}
    ${found}  Set variable  ${FALSE}
    FOR  ${dict}  IN  @{query_result}
        Log  ${dict}[ADDRESS]
        ${found}  Run keyword if  '${dict}[ADDRESS]'=='California'
        ...  Set variable  ${TRUE}
        ...  ELSE
        ...  Continue for loop
    END
    Should be true  ${found}

Get samples from target for comparison
    [Arguments]  ${samples_s}  ${cols_source}  ${cols_target}
    ${common_cols}  Evaluate  [value for value in ${cols_source} if value in ${cols_target}]
    @{cols}  Evaluate  ${cols_source} if len(${cols_source}) > len(${cols_target}) else ${cols_target}
    ${indexes}  Evaluate  [${cols}.index(item) for item in ${common_cols}]
    ${cols_str}  Evaluate  ", ".join(${common_cols})
    @{samples}  Create List
    FOR  ${line}  IN  @{samples_s}
        ${w_cond}  Evaluate  " AND ".join([${cols}\[i] + '=' + '"' + str(${line}\[i]) + '"' for i in ${indexes}])
        ${select}  Set Variable  SELECT ${cols_str} FROM ${TABLE} WHERE ${w_cond}
        ${query_result}  Query  ${select}  alias=target
        Append To List  ${samples}  ${query_result[0]}
    END
    [Return]  ${samples}

Get samples from target for comparison obsolete
    [Arguments]  ${samples_s}  ${cols_source}  ${common_cols}
    ${cols}  Evaluate  ", ".join(${common_cols})
    @{indexes}  Create List
    FOR  ${item}  IN  @{common_cols}
        ${ind}  Get Index From List  ${cols_source}  ${item}
        Append To List  ${indexes}  ${ind}
    END
    @{samples}  Create List
    FOR  ${line}  IN  @{samples_s}
        ${w_cond}  Evaluate  " AND ".join([${cols_source}\[i] + '=' + '"' + str(${line}\[i]) + '"' for i in ${indexes}])
        ${select}  Set Variable  SELECT ${cols} FROM ${TABLE} WHERE ${w_cond}
        ${query_result}  Query  ${select}  alias=target
        Append To List  ${samples}  ${query_result[0]}
    END
    [Return]  ${samples}

Get ignored column index
    [Arguments]  ${uncommon_cols}  ${cols_source}
    @{indexes}  Create List
    FOR  ${item}  IN  @{uncommon_cols}
        ${ind}  Get Index From List  ${cols_source}  ${item}
        Append To List  ${indexes}  ${ind}
    END
    [Return]  ${indexes}

Get ignored column index source and target
    [Arguments]  ${source}  ${target}
    ${uncommon_cols_source}  Evaluate  [value for value in ${source} if value not in ${target}]
    ${uncommon_cols_target}  Evaluate  [value for value in ${target} if value not in ${source}]
    ${indexes_source}  Evaluate  [${source}.index(item) for item in ${uncommon_cols_source}]
    ${indexes_target}  Evaluate  [${target}.index(item) for item in ${uncommon_cols_target}]
    [Return]  ${indexes_source}  ${indexes_target}

Get random sample from table
    [Arguments]  ${table}  ${percentage}  ${row_cnt}
    ${cnt}  Query  SELECT COUNT(*) FROM ${table}  alias=source
    ${size}  Evaluate  math.ceil(${row_cnt} * ${percentage} / ${100})
    ${samples_s}  Query  SELECT * FROM ${table} ORDER BY RANDOM() LIMIT ${size}   alias=source
    [Return]  ${samples_s}

Get table column names
    [Arguments]  ${table}  ${origin}
    ${table_properties}  Query  PRAGMA table_info('${table}')  returnAsDict=${TRUE}  alias=${origin}
    ${cols}  Evaluate  [item['name'] for item in ${table_properties}]
    [Return]  ${cols}

Get common column names
    [Arguments]  ${source}  ${target}
    ${common_cols}  Evaluate  [value for value in ${source} if value in ${target}]
    [Return]  ${common_cols}

Get uncommon column names
    [Arguments]  ${source}  ${target}
    ${uncommon_cols}  Evaluate  [value for value in ${source} if value not in ${target}] + [value for value in ${target} if value not in ${source}]
    [Return]  ${uncommon_cols}

Get row count
    [Arguments]  ${table}  ${origin}
    ${cnt}  Query  SELECT COUNT(*) FROM ${table}  alias=${origin}
    [Return]  ${cnt[0][0]}

Get all data from table
    [Arguments]  ${table}  ${origin}
    ${tbl}  Query  SELECT * FROM ${table}  alias=${origin}
    [Return]  ${tbl}

Get primary keys count
    [Arguments]  ${table}  ${col_name}  ${origin}
    ${prim_keys_cnt}  Query  SELECT ${col_name}, COUNT(*) AS counter FROM ${table} GROUP BY ${col_name} HAVING COUNT (*) > 1  alias=${origin}
    [Return]  ${prim_keys_cnt}