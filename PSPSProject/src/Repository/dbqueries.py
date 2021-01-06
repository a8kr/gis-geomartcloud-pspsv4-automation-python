class queries:
    # timeplace Info table
    get_timeplace = "select * from psps.timeplace_info where timeplacename = '%s'"
    get_activetablename = "select activetablename from psps.table_info where componentname = 's3-defaultmanagement-circuits'"


