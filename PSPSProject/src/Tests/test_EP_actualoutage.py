import csv
import os
import pandas as pd
import pytest
import geopandas as gpd
import numpy as np
import arcpy
from pathlib import Path
from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, readData, \
    downloadsfolderPath, deleteFolder, create_folder
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath
from PSPSProject.src.ReusableFunctions.parquetimporttool import ImportTool

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestEPActualOutage(BaseClass):
    def test_actualoutage(self):
        try:
            var_row = VAR_COUNT[0]
            var_os = VAR_COUNT[2]
        except(ValueError, Exception):
            exceptionrow = exceptionRowCount()
            var_row = exceptionrow
        log = self.getLogger(logfilepath, VAR_TESTCASENAME)
        homepage = HomePage(self.driver)
        defmanagement = DefaultManagement(self.driver)
        uielements = UI_Element_Actions(self.driver)
        deleteFiles(downloadsfolder, ".csv")
        var_execution_flag = ''
        final_assert = []
        log.info("Starting Validation")
        if var_os == "MAC-OS":
            homepage.SignOn()
            log.info("Successfully entered user id & password:")
            print("Successfully entered user id & pasnsword:")

        # Create temp folder to save artifacts
        actualvalidationfolder = downloadsfolderPath + "actualvalidationfolder"
        deleteFolder(actualvalidationfolder)
        create_folder(actualvalidationfolder)
        # get filenames from testdata sheet
        currentoutagesfilename = readData(testDatafilePath, "EP", var_row, 8)
        outagedevicesfilename = readData(testDatafilePath, "EP", var_row, 9)
        # get csv files from downloads folder and assign output filenames with headers
        # Current Outages
        currentoutagesfile = downloadsfolderPath + currentoutagesfilename
        currentoutagesfileop = actualvalidationfolder + '\\ff_criticalweb_current_outages_op.csv'
        currentoutagescsv = pd.read_csv(currentoutagesfile, sep="|", header=None)
        currentoutagescolumns = ["OUTAGE_ID", "OUTAGE_EXTENT", "OUTAGE_DEVICE_ID",
                                 "OUTAGE_DEVICE_NAME", "OUTAGE_CIRCUIT_ID", "OUTAGE_START",
                                 "CREW_ETA", "CREW_CURRENT_STATUS", "AUTO_ETOR", "CURRENT_ETOR",
                                 "OUTAGE_CAUSE", "EST_CUSTOMERS", "LAST_UPDATE", "HAZARD_FLAG",
                                 "OUTAGE_LATITUDE", "OUTAGE_LONGITUDE", "CITY", "COUNTY", "ZIP"]
        currentoutagescsv.columns = currentoutagescolumns
        currentoutagescsv.to_csv(currentoutagesfileop, index=False)
        currentoutagesdf = pd.read_csv(currentoutagesfileop)
        # Outage Devices
        outagedevicesfile = downloadsfolderPath + outagedevicesfilename
        outagedevicesfileop = actualvalidationfolder + '\\ff_criticalweb_outage_devices_op.csv'
        outagedevicescsv = pd.read_csv(outagedevicesfile, sep="|", header=None)
        outagedevicescsv.columns = ["#ID", "DEVICE_ID", "DEVICE_NAME", "LATITUDE", "LONGITUDE", "CIRCUIT_ID",
                                    "OUTAGE_ID", "DEVICE_TYPE_ID"]
        outagedevicescsv.to_csv(outagedevicesfileop, index=False)
        outagedevicesdf = pd.read_csv(outagedevicesfileop)
        # get fgdb files from downloads folder and convert to csv files in temp folder
        # Outage Location

        filegdb = downloadsfolderPath + '\\outage_location\\outage_location.gdb'
        gdf = gpd.read_file(filegdb, driver='FileGDB', layer='outage_location')
        outage_locationgdploc = actualvalidationfolder + '\\outage_locationgdp.csv'
        outage_locationgdp = gdf.to_csv(outage_locationgdploc, sep=',', encoding='utf-8')
        outage_locationgdpdf = pd.read_csv(outage_locationgdploc, sep=',')

        # Outage Polygon
        filegdb1 = downloadsfolderPath + '\\outage_polygon\\outage_polygon.gdb'
        gdf1 = gpd.read_file(filegdb1, driver='FileGDB', layer='outage_polygon')
        outage_polygongdploc = actualvalidationfolder + '\\outage_polygongdp.csv'
        outage_polygongdp = gdf1.astype({'geometry': str}).to_csv(outage_polygongdploc, sep=',', encoding='utf-8')
        # outage_polygongdp = gdf1.to_csv(outage_polygongdploc, sep=',', encoding='utf-8')
        outage_polygongdpdf = pd.read_csv(outage_polygongdploc, sep=',')

        # Get unique outage ids from devices and current outage csvs
        var_outageids_devices = outagedevicesdf['OUTAGE_ID'].unique()
        var_outageids_current = currentoutagesdf['OUTAGE_ID'].unique()

        # 1. Verify if all the Outage ID from Outage polygon gdb and outage devices csv and take outage ids which are
        # not coming in the Outage polygon gdb
        var_outages_polygongdp = outage_polygongdpdf['OUTAGE_ID'].unique()

        # mismatch_outages_polygongdp = np.setdiff1d(var_outages_polygongdp, var_outageids_devices)
        mismatch_outages_polygongdp = np.setdiff1d(var_outageids_devices, var_outages_polygongdp)
        if mismatch_outages_polygongdp.size == 0:
            log.info("There are no mismatch Outage ID values between devices csv and polygongdp")
        else:
            np.savetxt(actualvalidationfolder + '\\step1_mismatchoutages.csv', mismatch_outages_polygongdp,
                       delimiter=',', fmt='%d')
            log.info("Mismatch values between polygongdp and devices csv are : " + str(mismatch_outages_polygongdp))
            # 2. mismatch outageid - check in current_csv
            mismatchoutagesstep1df = [str(a) for a in mismatch_outages_polygongdp]
            mismatch_outages_currentfile = np.setdiff1d(mismatchoutagesstep1df, var_outageids_current)

            if mismatch_outages_currentfile.size != 0:
                # np.savetxt(actualvalidationfolder + '\\step2_mismatchoutages.csv', mismatch_outages_currentfile,delimiter=',', fmt='%d')
                np.savetxt(actualvalidationfolder + '\\step2_mismatchoutages.csv', mismatch_outages_currentfile,
                           delimiter=',', fmt='%s')
                log.error(
                    "Mismatch values between polygongdp and devices csv which are not present in current Outages csv :" + str(
                        mismatch_outages_currentfile))
            else:
                log.info("All the mismatch values b/w polygongdp and devices csv are present in current Outages csv")
                # 3 Check Cgc12 transformer id for mismatch values
                # Take CG12- DeviceID for mismatch records from devices csv
                matchedoutageids = np.intersect1d(mismatchoutagesstep1df, var_outageids_current)
                deviceiddf = outagedevicesdf[outagedevicesdf['OUTAGE_ID'].isin(matchedoutageids)]
                deviceiddf.to_csv(actualvalidationfolder + '\\step3_mismatchoutages_checkcgc12.csv', index=False)
                deviceiddf = deviceiddf['DEVICE_ID'].fillna('0', inplace=False)
                deviceids = [int(i) for i in deviceiddf]
                deviceids = np.unique(deviceids)
                print(deviceids)

                data_dir = Path(downloadsfolderPath + '\\transformerparque')
                full_df = pd.concat(
                    pd.read_parquet(parquet_file)
                    for parquet_file in data_dir.glob('*.parquet')
                )
                # full_df.to_csv(downloadsfolderPath + '\\parquecsv_file.csv')
                cgc12 = full_df['cgc12'].unique()
                cgc12s = [int(i) for i in cgc12]
                # full_df.to_csv(downloadsfolderPath + '\\parquecsv_file.csv')
                deviceids = [str(a) for a in deviceids]
                cgc12s = [str(a) for a in cgc12s]
                mismatch_cgfs = np.setdiff1d(deviceids, cgc12s)
                mismatch_cgfs = [str(a) for a in mismatch_cgfs]
                if mismatch_cgfs.sort() == deviceids.sort():
                    log.info("For mismatch outage ids, cgc12 is not present in transformer table")
                else:
                    log.error("For mismatch outage ids, cgc12 is present in transformer table")

        # Shape Validation
        params = ImportTool().getParameterInfo()
        params[1].value = downloadsfolderPath + '\\transformerparque'
        params[2].value = "transformerfeaturelayer"
        params[5].value = "Shape@WKB"
        params[6].value = "POLYGON"
        params[7].value = arcpy.SpatialReference(26910).exportToString()

        intersectedlayerfolder = downloadsfolderPath + "intersectedlayer"
        deleteFolder(intersectedlayerfolder)
        create_folder(intersectedlayerfolder)
        temp_transfeaturelayerpath = ImportTool().execute(params, '')
        lyr_outage_path = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\outage_polygon\outage_polygon.gdb\outage_polygon"

        # arcpy.analysis.Intersect([lyr_outage_path, temp_transfeaturelayerpath], r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\intersectlayer\intersectlayer", "ALL")
        arcpy.PairwiseIntersect_analysis([lyr_outage_path,temp_transfeaturelayerpath],
                                         r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\intersectedlayer\intersectedlayer")

        # geometry check within CA

        # lyr_transformers = arcpy.management.MakeFeatureLayer(temp_transfeaturelayerpath, "transformerfeaturelayer").getOutput(0)
        # lyr_outage = arcpy.management.MakeFeatureLayer(r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\outage_polygon\outage_polygon.gdb\outage_polygon", "outage_polygon").getOutput(0)

        #arcpy.management.MakeFeaureLayer(lyr_transformers, lyr_outage)
        #arcpy.analysis.Intersect(["outage_polygon", "transformerfeaturelayer"], "intersectedlayer", "ALL")
        #df = arcpy.mapping.ListLayers("intersectedlayer").dataSource
        #print(df)

        #lyr_transformers = arcpy.management.MakeFeatureLayer(temp_transfeaturelayerpath, "transformerfeaturelayer").getOutput(0)
        # lyr_outage_path = r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\outage_polygon\outage_polygon.gdb\outage_polygon"
        # aprx = arcpy.mp.ArcGISProject("CURRENT")
        # aprxMap = aprx.listMaps("MainMap")[0]
        # lyrFile_outage = arcpy.mp.LayerFile(lyr_outage_path)
        # lyrFile_transformers = arcpy.mp.LayerFile(temp_transfeaturelayerpath)
        # aprxMap.addLayer(lyrFile_outage)
        # aprxMap.addLayer(lyrFile_transformers)
        # #arcpy.management.AddLayer(lyr_transformers, lyr_outage)