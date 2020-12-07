import os
import pandas as pd
import numpy as np
import pytest
import geopandas as gpd

from PSPSProject.src.Pages.DefaultManagement import DefaultManagement
from PSPSProject.src.Pages.HomePage import HomePage
from PSPSProject.src.Repository.uilocators import locators
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import logfilepath, deleteFiles, readData, getCSVrowCount, \
    downloadsfolderPath, deleteFolder, create_folder, comparedf, timezoneconversion_utc, compare_two_columns_dataframe
from PSPSProject.src.ReusableFunctions.uiactions import UI_Element_Actions
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, testDatafolderPath

VAR_TESTCASENAME = os.path.basename(__file__)
global VAR_COUNT, VAR_TESTENV
VAR_COUNT = 0
VAR_TESTENV = ""
VAR_COUNT = BaseClass().InitializeExecution(VAR_TESTCASENAME, VAR_COUNT, VAR_TESTENV)


@pytest.mark.skipif(not VAR_COUNT, reason="Excluded from regression suite")
@pytest.mark.regression
class TestEPOutageLocation(BaseClass):
    def test_outagelocation(self):
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
            print("Successfully entered user id & password:")

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
        # get fgdb files from downloads folder and convert to csv files in temp folder
        # Outage Location
        filegdb = downloadsfolderPath + '\\outage_location\\outage_location.gdb'
        gdf = gpd.read_file(filegdb, driver='FileGDB', layer='outage_location')
        outage_locationgdploc = actualvalidationfolder + '\\outage_locationgdp.csv'
        outage_locationgdp = gdf.to_csv(outage_locationgdploc, sep=',', encoding='utf-8')
        outage_locationgdpdf = pd.read_csv(outage_locationgdploc, sep=',', index_col=0)
        outage_locationgdp1 = outage_locationgdpdf.drop('geometry', axis=1)
        outage_locationgdp1.to_csv(actualvalidationfolder + '\\outage_locationgdp_updated.csv')

        currentoutagesdf["OUTAGE_START"] = timezoneconversion_utc(currentoutagesdf["OUTAGE_START"])
        currentoutagesdf["CREW_ETA"] = timezoneconversion_utc(currentoutagesdf["CREW_ETA"])
        currentoutagesdf["AUTO_ETOR"] = timezoneconversion_utc(currentoutagesdf["AUTO_ETOR"])
        currentoutagesdf["CURRENT_ETOR"] = timezoneconversion_utc(currentoutagesdf["CURRENT_ETOR"])
        currentoutagesdf["LAST_UPDATE"] = timezoneconversion_utc(currentoutagesdf["LAST_UPDATE"])
        currentoutagesdf["OUTAGE_LATITUDE"] = currentoutagesdf['OUTAGE_LATITUDE'].fillna('0', inplace=False)
        currentoutagesdf["OUTAGE_LONGITUDE"] = currentoutagesdf['OUTAGE_LONGITUDE'].fillna('0', inplace=False)
        currentoutagesdf["EST_CUSTOMERS"] = currentoutagesdf['EST_CUSTOMERS'].fillna('0', inplace=False)
        currentoutagesdf["HAZARD_FLAG"] = currentoutagesdf['HAZARD_FLAG'].fillna('0', inplace=False)
        currentoutagesdf["OUTAGE_DEVICE_NAME"] = currentoutagesdf['OUTAGE_DEVICE_NAME'].str.replace(' ', '')
        currentoutagesdf["ZIP"] = currentoutagesdf['ZIP'].str.replace(' ', '')
        currentoutagesdf["ZIP"] = currentoutagesdf['ZIP'].fillna('0', inplace=False)
        currentoutagesdf.to_csv(actualvalidationfolder + '\\ff_criticalweb_current_outages_op_updated.csv')

        # Validate Outage location and devices data
        df = outage_locationgdp1.compare(currentoutagesdf, align_axis='index')
        mismatchdata = df.to_csv(actualvalidationfolder + '\\mismatchdata.csv')
        df1 = pd.read_csv(actualvalidationfolder + '\\mismatchdata.csv',index_col=0)
        mismatchdata1 = df1.replace(['self'], 'outagelocation_gdp')
        mismatchdata1 = mismatchdata1.replace(['other'], 'currentoutages_csv')
        mismatchdata1.to_csv(actualvalidationfolder + '\\outagelocation_mismatchdata.csv')
