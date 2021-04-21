class queries:
    # table_info table
    get_activetablename = "select activetablename from psps.table_info where componentname = '%s'"
    # timeplace Info table
    get_timeplace = "select * from psps.timeplace_info where timeplacename = '%s' order by timeplacetimestamp desc"
    get_tp_status = "Select status from psps.timeplace_info where timeplacename = '%s'"
    get_shape = "select * from dx_tp where time_place_id='%s'"
    get_scopeversion = "select objectid, time_place_id, shape, scope_version from dx_tp where scope_version='%s'"
