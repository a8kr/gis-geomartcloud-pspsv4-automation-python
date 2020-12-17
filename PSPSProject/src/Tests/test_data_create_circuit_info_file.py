
import arcpy
import geopandas as gpd
import psycopg2
import pytest
import os
import pandas as pd

from PSPSProject.src.Repository.dbqueries import queries
#from PSPSProject.src.ReusableFunctions.baseclass import testEnvironment
from PSPSProject.src.ReusableFunctions.parquetimporttool import ImportTool
from PSPSProject.src.Tests.conftest import config_met
from PSPSProject.src.ReusableFunctions.databasefunctions import queryresults_get_alldata_met, queryresults_get_alldata
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import readData, logfilepath, deleteFiles, downloadsfolderPath, \
    deleteFolder, create_folder

#get_timeplace_db = queries.get_timeplace #% timeplace

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestCircuitInfo(BaseClass):
    def test_circuitinfo(self):
        try:
            var_row = VAR_COUNT[0]
            var_os = VAR_COUNT[2]
        except(ValueError, Exception):
            exceptionrow = exceptionRowCount()
            var_row = exceptionrow
        log = self.getLogger(logfilepath, VAR_TESTCASENAME)
        deleteFiles(downloadsfolder, ".csv")
        var_tp_array = []
        var_execution_flag = ''
        final_assert = []
        log.info("Starting Validation")
        #var_row = VAR_COUNT[0]
        timeplace = readData(testDatafilePath, "Data", var_row, 9)  #15
        if timeplace is None or timeplace == "":
             print("Version should take from UI")
        else:
             var_tp_array.append(timeplace)
        config_met()
        get_timeplace_db = queries.get_scopeversion %timeplace  #2020-11-07-V-01
        print(get_timeplace_db)
        lst_tp_details = queryresults_get_alldata_met(get_timeplace_db)
        #print(lst_tp_details)
        var_tp_Objectid = lst_tp_details[0][0]
        var_tp_id = lst_tp_details[0][1]
        log.info("Timeplace UID for timeplace: " + str(timeplace) + " is: " + str(var_tp_Objectid))
        log.info("Timeplace ID for timeplace: " + str(timeplace) + " is: " + str(var_tp_id))
        log.info("-----------------------------------------------------------------------------------------------")
        TimePlacedataloc = downloadsfolderPath + "\\Timeplace.csv"
        df_TimePlace = pd.DataFrame(lst_tp_details, columns = ['Objectid', 'time_place_id', 'shape', 'scope_version'])
        df_TimePlace.to_csv(path_or_buf=TimePlacedataloc)
        print("till jere")
        con = psycopg2.connect(database=config_met()['database'], user=config_met()['user'],
                               password=config_met()['password'], host=config_met()['host'])
        df = gpd.GeoDataFrame.from_postgis(get_timeplace_db, con, geom_col='shape')
        gdf = gpd.GeoDataFrame(df, geometry='shape', crs={'init': 'epsg:26910'})
        meterologyshapeloc = downloadsfolderPath + "\\meterologyshape"
        deleteFolder(meterologyshapeloc)
        create_folder(meterologyshapeloc)
        metfilename = meterologyshapeloc + "/" + "meterologyresult.shp"
        gdf.to_file(driver='ESRI Shapefile', filename=metfilename)

        # df_timeplacecircuits.createOrReplaceTempView("timeplace_circuits")
        # tempfolder = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\actualtimeplace_circuits"
        # df_timeplacecircuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(
        #     tempfolder)
        # inFeatures = [lst_tp_details, hfra_layer]
        # intersectOutput = "HFRA_Layer_Intersect"
        # arcpy.Intersect_analysis(inFeatures, intersectOutput)
        ## try the unique feature classification and ignore the nulls during intersect_analysis.
        #### New from here ####

        params = ImportTool().getParameterInfo()
        params[1].value =  downloadsfolderPath + '\\hfraparque'    ## + '\\hfraparque'
        params[2].value = "hfrafeaturelayer"
        params[5].value = "shape@WKB"
        params[6].value = "POLYGON"
        params[7].value = arcpy.SpatialReference(26910).exportToString()

        intersectedlayerfolder = downloadsfolderPath + "intersectedlayer"
        deleteFolder(intersectedlayerfolder)
        create_folder(intersectedlayerfolder)
        temp_hfralayerpath = ImportTool().execute(params, '')
        print(temp_hfralayerpath)
        # intersect_analysis or analysis.intersect
        # inFeatures = [
        #     r"\\utility.pge.com\users\personal\H2MN\My Documents\ArcGIS\Projects\MyProject\MyProject.gdb\Dx_TP",
        #     r"\\utility.pge.com\users\personal\H2MN\My Documents\ArcGIS\Projects\MyProject\MyProject.gdb\HFRA_Layer"]
        inFeatures = [metfilename, temp_hfralayerpath]
        #r'C:\Hema_Workspace\gis - geomartcloud - pspsv4 - automation - python\PSPSProject\downloads \Timeplace'
        intersectOutput = intersectedlayerfolder + "\\HFRA_Layer_Intersect"
        print("hi")
        arcpy.Intersect_analysis(inFeatures, intersectOutput)
        print("Intersectedhfra_layer")
        #arcpy.analysis.Intersect(inFeatures, intersectOutput)

        # final_assert.append(False)
        #
        # if False in final_assert:
        #     log.error("One of Test Case Execution Failed")
        # else:
        #     log.info("All Test Cases Executed successfully ")
        #
        # if var_execution_flag == 'fail':
        #     log.error("Execution failed: Errors found in execution!!")
        #     assert False
        # log.info("----------------------------------------------------------------------------------------------")
        # log.info("*************AUTOMATION EXECUTION COMPLETED*************")
