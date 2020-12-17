class queries:
    # table_info table
    get_activetablename = "select activetablename from psps.table_info where componentname = '%s'"
    # timeplace Info table
    get_timeplace = "select * from psps.timeplace_info where timeplacename = '%s' order by timeplace_uid asc"







