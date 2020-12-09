class queries:
    # Version Info table
    get_versionid = "select * from psps.versioninfo where versionname = '%s'"
    get_activetablename = "select activetablename from psps.table_info where componentname = 's3-defaultmanagement-circuits'"