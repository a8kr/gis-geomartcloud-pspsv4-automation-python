class queries:
    # timeplace Info table
    get_timeplace = "select * from psps.timeplace_info where timeplacename = '%s'"
    get_scopeversion = "select objectid, time_place_id, shape, scope_version from dx_tp where scope_version='%s'"




