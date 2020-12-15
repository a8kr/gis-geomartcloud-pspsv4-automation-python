import os
import pytest
import boto3
import pandas as pd
import geopandas as gpd
import numpy as np
import arcpy
from pathlib import Path

import shapefile

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.ReusableFunctions.awsfunctions import download_file_from_S3
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, readData, \
    downloadsfolderPath, deleteFolder, create_folder, unzip_file
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, s3config
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

        # Download all the required data files from S3
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['emergwebbucketname']
        BUCKET_PATH = s3config()['outagepath']
        profilename = s3config()['profile_name']
        # Download outage location.zip file
        filename = "outage_location.zip"
        localpath = downloadsfolderPath + "\\outages" + "\\" + filename
        tempoutages = downloadsfolderPath + "outages"
        deleteFolder(tempoutages)
        create_folder(tempoutages)
        download_file_from_S3(s3_bucketname, BUCKET_PATH, filename, localpath, profilename)
        log.info("Downloaded outage location.zip folder from S3")
        unzipmainfile = unzip_file(localpath, tempoutages)
        log.info("Unzipped outage location folder")
        # Download outage location.zip file
        filename1 = "outage_polygon.zip"
        localpath1 = downloadsfolderPath + "\\outages" + "\\" + filename1
        download_file_from_S3(s3_bucketname, BUCKET_PATH, filename1, localpath1, profilename)
        log.info("Downloaded outage polygon.zip folder from S3")
        unzipmainfile1 = unzip_file(localpath1, tempoutages)
        log.info("Unzipped outage location folder")
        # Download outage location and Outage Polygon csv files
        BUCKET_PATH1 = s3config()['outagecsvpath']
        currentoutagesfilename = readData(testDatafilePath, "EP", var_row, 8)
        outagedevicesfilename = readData(testDatafilePath, "EP", var_row, 9)
        currentoutagesfile = downloadsfolderPath + "\\outages" + "\\" + currentoutagesfilename
        download_file_from_S3(s3_bucketname, BUCKET_PATH1, currentoutagesfilename, currentoutagesfile, profilename)
        log.info("Downloaded Current Outages file from S3")
        outagedevicesfile = downloadsfolderPath + "\\outages" + "\\" + outagedevicesfilename
        download_file_from_S3(s3_bucketname, BUCKET_PATH1, outagedevicesfilename, outagedevicesfile, profilename)
        log.info("Downloaded Outage Devices file from S3")

        # Create temp folder to save artifacts
        actualvalidationfolder = downloadsfolderPath + "actualvalidationfolder"
        deleteFolder(actualvalidationfolder)
        create_folder(actualvalidationfolder)
        # get csv files from downloads folder and assign output filenames with headers
        # Current Outages
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
        outagedevicesfileop = actualvalidationfolder + '\\ff_criticalweb_outage_devices_op.csv'
        outagedevicescsv = pd.read_csv(outagedevicesfile, sep="|", header=None)
        outagedevicescsv.columns = ["#ID", "DEVICE_ID", "DEVICE_NAME", "LATITUDE", "LONGITUDE", "CIRCUIT_ID",
                                    "OUTAGE_ID", "DEVICE_TYPE_ID"]
        outagedevicescsv.to_csv(outagedevicesfileop, index=False)
        outagedevicesdf = pd.read_csv(outagedevicesfileop)
        # get fgdb files from downloads folder and convert to csv files in temp folder
        # Outage Location

        filegdb = tempoutages + '\\outage_location.gdb'
        gdf = gpd.read_file(filegdb, driver='FileGDB', layer='outage_location')
        outage_locationgdploc = actualvalidationfolder + '\\outage_locationgdp.csv'
        outage_locationgdp = gdf.to_csv(outage_locationgdploc, sep=',', encoding='utf-8')
        outage_locationgdpdf = pd.read_csv(outage_locationgdploc, sep=',')

        # Outage Polygon
        filegdb1 = tempoutages + '\\outage_polygon.gdb'
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
        log.info("Transformer table is imported and parque file to shape conversion is completed")
        lyr_outage_path = filegdb1 + '\\outage_polygon'
        log.info("Intersection on transformerfeaturelayer and outage_polygon started:")
        # arcpy.analysis.Intersect([lyr_outage_path, temp_transfeaturelayerpath], r"C:\PSPSViewerV4.0_GIT\gis-geomartcloud-pspsv4-automation-python\PSPSProject\downloads\intersectlayer\intersectlayer", "ALL")
        intersect = arcpy.PairwiseIntersect_analysis([lyr_outage_path, temp_transfeaturelayerpath], intersectedlayerfolder + '\\intersectedlayer')
        files = os.listdir(intersectedlayerfolder)
        # Read Intersected shape file
        for file in files:
            if file.endswith('shp'):
                break
        shapefileloc = intersectedlayerfolder + '\\' + file
        print(shapefileloc)
        # Read Shape file and get the attributes
        sf = shapefile.Reader(shapefileloc)
        shaperecords = sf.records()
        log.info("No of records present in intersected layer are: "+str(len(shaperecords)))
        if len(shaperecords) > 1:
            log.info("Intersection done on transformerfeaturelayer and outage_polygon gdp")
        else:
            log.info("Intersection not done on transformerfeaturelayer and outage_polygon gdp")
        # geometry check within CA
