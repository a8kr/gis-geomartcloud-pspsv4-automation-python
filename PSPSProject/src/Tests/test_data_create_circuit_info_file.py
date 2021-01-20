import time

import boto3
import pytest
import geopandas as gpd
import psycopg2
import pandas as pd
import arcpy
import numpy as np
import os

from pyspark.sql import SparkSession
from shapely import wkt
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

from PSPSProject.src.Repository.dbqueries import queries
from PSPSProject.src.ReusableFunctions.awsfunctions import download_dir_from_S3
from PSPSProject.src.ReusableFunctions.parquetimporttool import ImportTool
from PSPSProject.src.ReusableFunctions.databasefunctions import queryresults_get_alldata_met, queryresults_fetchone
from PSPSProject.src.Tests.conftest import downloadsfolder, testDatafilePath, config_met, s3config
from PSPSProject.src.ReusableFunctions.baseclass import BaseClass, exceptionRowCount
from PSPSProject.src.ReusableFunctions.commonfunctions import readData, logfilepath, deleteFiles, downloadsfolderPath, \
    deleteFolder, create_folder

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
        # var_row = VAR_COUNT[0]
        timeplace = readData(testDatafilePath, "Data", var_row, 9)  # 15
        if timeplace is None or timeplace == "":
            print("Version should take from UI")
        else:
            var_tp_array.append(timeplace)

        #Get Data from Meterology database and convert to shape file
        get_timeplace_db = queries.get_scopeversion % timeplace  # 2020-11-07-V-01
        print(get_timeplace_db)
        con = psycopg2.connect(database=config_met()['database'], user=config_met()['user'],
                               password=config_met()['password'], host=config_met()['host'])
        df = gpd.GeoDataFrame.from_postgis(get_timeplace_db, con, geom_col='shape')
        gdf = gpd.GeoDataFrame(df, geometry='shape', crs={'init': 'epsg:26910'})
        log.info("Data from Meteorology db is retrieved for the scope version: " + timeplace)
        meterologyshapeloc = downloadsfolderPath + "\\meterologyshape"
        deleteFolder(meterologyshapeloc)
        create_folder(meterologyshapeloc)
        metfilename = meterologyshapeloc + "/" + "meterologyresult.shp"
        gdf.to_file(driver='ESRI Shapefile', filename=metfilename)
        log.info("Conversion of Meteorology data to shape is completed and stored in meterologyshape folder")

        # Get the latest HFRA table filepath from db
        get_hfratable_db = queries.get_activetablename % 's3-hfra'
        lst_hfratable_details = queryresults_fetchone(get_hfratable_db)
        filename_hfratable = lst_hfratable_details
        log.info("Filename for HFRA active table is : " + str(filename_hfratable))
        log.info("-----------------------------------------------------------------------------------------------")
        hfratablefilename = filename_hfratable.split(s3config()['devpath'])[-1]
        print(hfratablefilename)

        # Download HFRA parquet file from S3 bucket
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        s3_bucketname = s3config()['pspsdatabucketname']
        hfraBUCKET_PATH = hfratablefilename
        profilename = s3config()['profile_name']
        hfra = downloadsfolderPath + "\\hfra"
        deleteFolder(hfra)
        create_folder(hfra)
        download_dir_from_S3(hfraBUCKET_PATH, s3_bucketname, profilename, hfra)
        log.info("Downloaded hfra parquet file from S3")

        time.sleep(10)
        log.info("Starting to load HFRA layer")
        # Convert HFRA Parquet file to shape file
        hfra = downloadsfolderPath + "\\hfra"
        hrfafile = hfra + "/" + "dev/data/pspsdatasync-v4/parquet/onetimeload"
        params = ImportTool().getParameterInfo()
        params[1].value = hrfafile
        params[2].value = "hfrafeaturelayer"
        params[5].value = "shape@WKB"
        params[6].value = "POLYGON"
        params[7].value = arcpy.SpatialReference(26910).exportToString()

        hfraintersectedlayerfolder = downloadsfolderPath + "/" + "hfraintersectedlayer"
        deleteFolder(hfraintersectedlayerfolder)
        create_folder(hfraintersectedlayerfolder)
        temp_hfrafeaturelayerpath = ImportTool().execute(params, '')

        # Do intersect on Meterology shape and HFRA layer
        arcpy.Intersect_analysis([metfilename, temp_hfrafeaturelayerpath], hfraintersectedlayerfolder + "\\intersectedlayer")
        log.info("Intersection done on Meterology tp and HFRA layer")

        time.sleep(20)
        log.info("Starting to load OH Conductor file")
        PriOHConductor = downloadsfolderPath + "\\PriOHConductor"
        PriOHConductorfile = PriOHConductor + "/" + "dev/data/pspsdatasync-v4/parquet/1214_0140"
        params = ImportTool().getParameterInfo()
        params[1].value = PriOHConductorfile
        params[2].value = "PriOHConductorlayer"
        params[5].value = "shape"
        params[6].value = "POLYLINE"
        params[7].value = arcpy.SpatialReference(26910).exportToString()

        hfrapriohintersectedlayerfolder = downloadsfolderPath + "/" + "hfrapriohintersected"
        deleteFolder(hfrapriohintersectedlayerfolder)
        create_folder(hfrapriohintersectedlayerfolder)
        temp_priohintersectedlayerfolder = ImportTool().executemodified(params, '')

        # Do intersect on Meterology shape and HFRA layer
        intersectedhfrafilename = hfraintersectedlayerfolder + "/" + "intersectedlayer.shp"
        arcpy.Intersect_analysis([intersectedhfrafilename, temp_priohintersectedlayerfolder],
                                 hfrapriohintersectedlayerfolder + "\\intersectedlayer")
        log.info("Intersection done on Intersected HFRA Layer and PriOHConductor layer")


        time.sleep(10)
        log.info("Starting to load Feeder Network Table file")
        feedernetwork = downloadsfolderPath + "\\feedernetwork"
        ffnwfile = feedernetwork + "/" + "dev/data/pspsdatasync-v4/parquet/1214_0140"
        params = ImportTool().getParameterInfo()
        params[1].value = ffnwfile
        params[2].value = "Feedernetworktable"
        params[6].value = "POINT"
        params[7].value = arcpy.SpatialReference(26910).exportToString()

        feedernetworktablefolder = downloadsfolderPath + "/" + "feedernetworktable"
        deleteFolder(feedernetworktablefolder)
        create_folder(feedernetworktablefolder)
        temp_feedernetworktablepath = ImportTool().execute(params, '')
        print (temp_feedernetworktablepath)
        # use the shp to csv conversion arcpy.tabletotableconversion method here
        arcpy.TableToTable_conversion(temp_feedernetworktablepath, feedernetworktablefolder, "ffnwtt.csv")
        log.info("Converted the FeederNetworkTable shape file into CSV file successfully")

        spark = SparkSession.builder.appName("Timeplace-Creation") \
            .config('spark.driver.memory', '10g') \
            .config("spark.cores.max", "6") \
            .config('spark.yarn.appMasterEnv.ARROW_PRE_0_15_IPC_FORMAT', 1) \
            .config('spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT', 1) \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.sql.catalogImplementation", "in-memory") \
            .getOrCreate()
        log.info("Spark session connected")

       # df_ohlayerintrscted = pd.read_csv(filepath_or_buffer= hfrapriohintersectedlayerfolder + "/" + "ohlayer1intrsct.csv", sep = '[$, |_]', engine = 'python')
        # print(df_ohlayerintrscted.head(5))
        # for col in df_ohlayerintrscted.columns:
        #     print(col)
        df_new_OH = spark.read.csv(hfrapriohintersectedlayerfolder + "/" + "ohlayer1intrsct.csv", header=True)
        df_new_OH.repartition(1).write.mode('overwrite').parquet(downloadsfolderPath + '\\int-oh')
        log.info("Converted Intermediate meteorology csv to parquet file")

        temp_df_new_OH = spark.read.parquet(downloadsfolderPath + '\\int-oh')
        temp_df_new_OH.createOrReplaceTempView("temp_intersected_OH")

        temp_ff_new = spark.read.parquet(ffnwfile)
        temp_ff_new.createOrReplaceTempView("temp_feeder_network")

        df_join_ssd = spark.sql(
            """SELECT s.feederid AS circuitid, s.operatingnumber AS opnum, s.assettype AS devicetype,s.circuitname, s.min_branch, s.max_branch, s.treelevel, s.order_num,'' AS fireindex, s.substationname, 'N' AS consider, s.operable, s.OPERABLE_TRANS_PMETER_COUNT FROM temp_feeder_network s INNER JOIN temp_intersected_OH v ON s.feederid = LPAD(split(v.ssd,'\\\\$')[0],9,'0') and s.operatingnumber = split(v.ssd,'\\\\$')[1] AND s.assettype = split(v.ssd,'\\\\$')[2]""")

        df_join_ssd.createOrReplaceTempView("df_new_ssd")
        tempfolder = downloadsfolderPath + "/" + "Join_FNetworkTable_OH"
        df_join_ssd.coalesce(1).write.option("header", "true").format("csv").mode(
            "overwrite").save(tempfolder)
        df_join_ssd.cache()
        log.info("Join function completed")

        cktsourcefolder = downloadsfolderPath + "\\cktsource"
        #cktsourcefile = cktsourcefolder + "/" + "dev/data/pspsdatasync-v4/parquet/1214_0140"
        temp_ckt_source = spark.read.parquet(cktsourcefolder)
        temp_ckt_source.createOrReplaceTempView("Temp_Ckt_Source")

        tempfolder = downloadsfolderPath + "\\Join_FNetworkTable_OH"
        ActiveCktJoinTable = downloadsfolderPath + "\\ActiveCktJoinTable"
        deleteFolder(ActiveCktJoinTable)
        create_folder(ActiveCktJoinTable)
        df_join_nwtable = spark.read.csv(tempfolder + "/" + "joined_feeder_network_table.csv", header=True)
        df_join_nwtable.repartition(1).write.mode('overwrite').parquet(downloadsfolderPath + '\\ActiveCktJoinTable')
        log.info("Converted Join Network Table csv to parquet file")

        temp_feedNWT_new = spark.read.parquet(ActiveCktJoinTable)
        temp_feedNWT_new.createOrReplaceTempView("Temp_Join_FeedNWT")

        df_join_ssd_active_circuits = spark.sql("""SELECT s.circuitid, s.opnum, s.devicetype, s.circuitname, s.min_branch, s.max_branch, s.treelevel, s.order_num, s.substationname, s.consider, s.operable, s.OPERABLE_TRANS_PMETER_COUNT, v.STATUS FROM Temp_Join_FeedNWT s LEFT JOIN Temp_Ckt_Source v ON LPAD(s.circuitid,9,'0') = v.CIRCUITID AND v.STATUS = '5'""")

        df_join_ssd_active_circuits.createOrReplaceTempView("df_new_ssd_active_circuits")
        # print (df_join_ssd_active_circuits.head(5))
        log.info("Eliminating the Inactive circuits and only keeping the active circuits")
        tempfolder_active_circuits = downloadsfolderPath + "/" + "Join_FNetworkTable_OH_Active_circuits"
        deleteFolder(tempfolder_active_circuits)
        create_folder(tempfolder_active_circuits)
        df_join_ssd_active_circuits.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save(tempfolder_active_circuits)
        df_join_ssd_active_circuits.cache()
        log.info("Join function completed with active circuits")

        df_ckt_file = pd.read_csv(tempfolder_active_circuits + "/" + "Join_Feednwtable_Active_Ckts.csv")
        print (df_ckt_file.nunique())

        tempfolder_final_output = downloadsfolderPath + "/" + "Final_output"
        df_ckt_final_file = pd.read_csv(tempfolder_final_output + "/" + "Final_Result.csv")
        print(df_ckt_final_file.nunique())

        df_ckt_out_table = spark.read.csv(tempfolder_active_circuits + "/" + "Join_Feednwtable_Active_Ckts.csv", header=True)
        df_ckt_out_table.repartition(1).write.mode('overwrite').parquet(downloadsfolderPath + '\\OutputCktTable')
        log.info("Converted Final Circuit Table csv to parquet file")

        OutputCktTable = downloadsfolderPath + '\\OutputCktTable'
        temp_op_ckt_table = spark.read.parquet(OutputCktTable)
        temp_op_ckt_table.createOrReplaceTempView("Temp_Output_CktTable")

        output_file = downloadsfolderPath + "\\ResultFilewithduplicates"
        verify_output_file = output_file + "/" + "outfile.csv"
        deleteFolder(output_file)
        create_folder(output_file)
        list_unique_ckts = list(df_ckt_file['circuitid'].unique())
        for i in range (len(list_unique_ckts)):
            if (list_unique_ckts[i]):
                 new_df = pd.DataFrame(df_ckt_file.loc[((df_ckt_file['circuitid'] == list_unique_ckts[i]), ['circuitid', 'opnum', 'devicetype', 'circuitname', 'min_branch', 'max_branch', 'treelevel', 'order_num', 'substationname', 'consider', 'operable', 'OPERABLE_TRANS_PMETER_COUNT', 'STATUS'])])
                 min_b = new_df['min_branch'].min()
                 max_b = new_df['max_branch'].max()
                 min_t = new_df['treelevel'].min()
                 max_o = new_df['order_num'].max()
                 all_df = pd.DataFrame(new_df.loc[(((new_df['min_branch'] == min_b) & (new_df['max_branch'] == max_b) & (new_df['treelevel'] == min_t) & (new_df['order_num'] == max_o)), ['circuitid', 'opnum', 'devicetype', 'circuitname', 'min_branch', 'max_branch', 'treelevel', 'order_num', 'substationname', 'consider', 'operable', 'OPERABLE_TRANS_PMETER_COUNT', 'STATUS'])])

                 df_min = pd.DataFrame(new_df.loc[((new_df['min_branch'] == new_df['min_branch'].min()), ['circuitid', 'opnum', 'devicetype', 'circuitname', 'min_branch', 'max_branch', 'treelevel', 'order_num', 'substationname', 'consider', 'operable', 'OPERABLE_TRANS_PMETER_COUNT', 'STATUS'])])
                 if (df_min.shape[0] > 1):
                     mn_b = df_min['min_branch'].min()
                     mx_b = df_min['max_branch'].max()
                     mn_t = df_min['treelevel'].min()
                     mx_o = df_min['order_num'].max()
                     update_df_min = pd.DataFrame(df_min.loc[(((df_min['min_branch'] == mn_b) & (df_min['max_branch'] == mx_b) & (df_min['treelevel'] == mn_t) & (df_min['order_num'] == mx_o)),
                                                                                                                    [
                                                                                                                        'circuitid',
                                                                                                                        'opnum',
                                                                                                                        'devicetype',
                                                                                                                        'circuitname',
                                                                                                                        'min_branch',
                                                                                                                        'max_branch',
                                                                                                                        'treelevel',
                                                                                                                        'order_num',
                                                                                                                        'substationname',
                                                                                                                        'consider',
                                                                                                                        'operable',
                                                                                                                        'OPERABLE_TRANS_PMETER_COUNT',
                                                                                                                        'STATUS'])])

                     df_min = update_df_min
                 df_max = pd.DataFrame(new_df.loc[((new_df['max_branch'] == new_df['max_branch'].max()),
                                                       ['circuitid', 'opnum', 'devicetype', 'circuitname', 'min_branch',
                                                        'max_branch', 'treelevel', 'order_num', 'substationname',
                                                        'consider', 'operable', 'OPERABLE_TRANS_PMETER_COUNT',
                                                        'STATUS'])])
                 if (df_max.shape[0] > 1):
                     mn_brch = df_max['min_branch'].min()
                     mx_brch = df_max['max_branch'].max()
                     mn_tree = df_max['treelevel'].min()
                     mx_ornum = df_max['order_num'].max()
                     update_df_max = pd.DataFrame(df_max.loc[(((df_max['min_branch'] == mn_brch) & (df_max['max_branch'] == mx_brch) & (df_max['treelevel'] == mn_tree) & (df_max['order_num'] == mx_ornum)),
                                                                                                                    [
                                                                                                                        'circuitid',
                                                                                                                        'opnum',
                                                                                                                        'devicetype',
                                                                                                                        'circuitname',
                                                                                                                        'min_branch',
                                                                                                                        'max_branch',
                                                                                                                        'treelevel',
                                                                                                                        'order_num',
                                                                                                                        'substationname',
                                                                                                                        'consider',
                                                                                                                        'operable',
                                                                                                                        'OPERABLE_TRANS_PMETER_COUNT',
                                                                                                                        'STATUS'])])

                     df_max = update_df_max
                 df_tree_level = pd.DataFrame(new_df.loc[((new_df['treelevel'] == new_df['treelevel'].min()),
                                                       ['circuitid', 'opnum', 'devicetype', 'circuitname', 'min_branch',
                                                        'max_branch', 'treelevel', 'order_num', 'substationname',
                                                        'consider', 'operable', 'OPERABLE_TRANS_PMETER_COUNT',
                                                        'STATUS'])])
                 if (df_tree_level.shape[0] > 1):
                     mn_b_t = df_tree_level['min_branch'].min()
                     mx_b_t = df_tree_level['max_branch'].max()
                     mn_t_t = df_tree_level['treelevel'].min()
                     mx_o_t = df_tree_level['order_num'].max()
                     update_df_tree_level = pd.DataFrame(df_tree_level.loc[(((df_tree_level['min_branch'] == mn_b_t) & (
                                 df_tree_level['max_branch'] == mx_b_t) & (df_tree_level['treelevel'] == mn_t_t) & (
                                                                           df_tree_level['order_num'] == mx_o_t)),
                                                              [
                                                                  'circuitid',
                                                                  'opnum',
                                                                  'devicetype',
                                                                  'circuitname',
                                                                  'min_branch',
                                                                  'max_branch',
                                                                  'treelevel',
                                                                  'order_num',
                                                                  'substationname',
                                                                  'consider',
                                                                  'operable',
                                                                  'OPERABLE_TRANS_PMETER_COUNT',
                                                                  'STATUS'])])

                     df_tree_level = update_df_tree_level
                 df_order_num = pd.DataFrame(new_df.loc[((new_df['order_num'] == new_df['order_num'].max()),
                                                       ['circuitid', 'opnum', 'devicetype', 'circuitname', 'min_branch',
                                                        'max_branch', 'treelevel', 'order_num', 'substationname',
                                                      'consider', 'operable', 'OPERABLE_TRANS_PMETER_COUNT',
                                                        'STATUS'])])
                 if (df_order_num.shape[0] > 1):
                     mn_b_o = df_order_num['min_branch'].min()
                     mx_b_o = df_order_num['max_branch'].max()
                     mn_t_o = df_order_num['treelevel'].min()
                     mx_o_o = df_order_num['order_num'].max()
                     update_df_order_num = pd.DataFrame(df_order_num.loc[(((df_order_num['min_branch'] == mn_b_o) & (
                                 df_order_num['max_branch'] == mx_b_o) & (df_order_num['treelevel'] == mn_t_o) & (
                                                                           df_order_num['order_num'] == mx_o_o)),
                                                              [
                                                                  'circuitid',
                                                                  'opnum',
                                                                  'devicetype',
                                                                  'circuitname',
                                                                  'min_branch',
                                                                  'max_branch',
                                                                  'treelevel',
                                                                  'order_num',
                                                                  'substationname',
                                                                  'consider',
                                                                  'operable',
                                                                  'OPERABLE_TRANS_PMETER_COUNT',
                                                                  'STATUS'])])

                     df_order_num = update_df_order_num
                 Merge_DF = pd.concat([df_min, df_max, df_tree_level, df_order_num])
                 new_df = new_df.sort_values(by = ['min_branch'])
                 Remove_duplicates_Merge_DF = pd.DataFrame(Merge_DF.drop_duplicates())
                 # print(Remove_duplicates_Merge_DF.head(5))
                 Remove_duplicates_Merge_DF_CB = pd.DataFrame(Remove_duplicates_Merge_DF.loc[
                                                                (Remove_duplicates_Merge_DF['opnum'] == 'CB') & (
                                                                            Remove_duplicates_Merge_DF[
                                                                                'circuitid'] == list_unique_ckts[i])])

                 log.info("Updated Remove_duplicates_Merge_DF_CB")
                 if not Remove_duplicates_Merge_DF_CB.empty:
                       with open(verify_output_file, 'a') as f:
                               Remove_duplicates_Merge_DF_CB.to_csv(f, header=False)

                 Merge_DF_sort_min_branch = Remove_duplicates_Merge_DF.sort_values(by = ['min_branch'])
                 # print(Merge_DF_sort_min_branch.head(5))
                 list_min_branch = (list(Remove_duplicates_Merge_DF['min_branch'].unique()))
                 list_max_branch = list(Remove_duplicates_Merge_DF['max_branch'].unique())
                 sorted_min_branch = sorted(list_min_branch)
                 sorted_max_branch = sorted(list_max_branch)
                 if Remove_duplicates_Merge_DF_CB.empty:
                     length = Remove_duplicates_Merge_DF.shape[0]
                     m = 0
                     while m < length:
                         log.info("Loop for Remove_duplicates_Merge_DF is starting now... And Length is updated")
                         if not (Remove_duplicates_Merge_DF.iloc[m].empty):
                             for n in range (new_df.shape[0]):
                                  log.info("Loop for New_DF is starting now...")
                                  # print (new_df.shape[0])
                                  if ((not((new_df.iloc[n]['min_branch'] <= Merge_DF_sort_min_branch.iloc[m]['max_branch']) and (new_df.iloc[n]['min_branch'] >= Merge_DF_sort_min_branch.iloc[m]['min_branch'])))):
                                         new_min_value = new_df.iloc[n]['min_branch']
                                         new_max_value = new_df.iloc[n]['max_branch']
                                         if ((new_min_value in sorted_min_branch) and (new_max_value in sorted_max_branch) and ((new_df.iloc[n]['min_branch'] <= Merge_DF_sort_min_branch.iloc[m]['max_branch']) and (new_df.iloc[n]['min_branch'] >= Merge_DF_sort_min_branch.iloc[m]['min_branch']))):
                                             new_df = pd.DataFrame(new_df.loc[((new_df['min_branch'] > new_min_value), ['circuitid', 'opnum', 'devicetype', 'circuitname', 'min_branch', 'max_branch', 'treelevel', 'order_num', 'substationname', 'consider', 'operable', 'OPERABLE_TRANS_PMETER_COUNT', 'STATUS'])])
                                             new_df_sorted = new_df.sort_values(by=['min_branch'])
                                             new_df = new_df_sorted
                                             log.info("New DF size is updated")
                                             break
                                         if not ((new_min_value in sorted_min_branch) and (new_max_value in sorted_max_branch) and ((new_df.iloc[n]['min_branch'] <= Merge_DF_sort_min_branch.iloc[m]['max_branch']) and (new_df.iloc[n]['min_branch'] >= Merge_DF_sort_min_branch.iloc[m]['min_branch']))):
                                             # print (Remove_duplicates_Merge_DF.iloc[m]['max_branch'])
                                             Remove_duplicates_Merge_DF_new_df = pd.DataFrame(new_df.loc[((new_df['min_branch'] == new_min_value),
                                                             ['circuitid', 'opnum', 'devicetype',
                                                              'circuitname', 'min_branch',
                                                              'max_branch', 'treelevel', 'order_num',
                                                              'substationname', 'consider',
                                                              'operable',
                                                              'OPERABLE_TRANS_PMETER_COUNT',
                                                              'STATUS'])])
                                             log.info("Remove_duplicates_Merge_DF_new_df is updated")
                                             # print(Remove_duplicates_Merge_DF_new_df)
                                             #new logic addition here
                                             if (Remove_duplicates_Merge_DF_new_df.shape[0] > 1):
                                                 min_brch = Remove_duplicates_Merge_DF_new_df['min_branch'].min()
                                                 max_brch = Remove_duplicates_Merge_DF_new_df['max_branch'].max()
                                                 min_tree = Remove_duplicates_Merge_DF_new_df['treelevel'].min()
                                                 max_ornum = Remove_duplicates_Merge_DF_new_df['order_num'].max()
                                                 update_Remove_duplicates_Merge_DF_new_df = pd.DataFrame(Remove_duplicates_Merge_DF_new_df.loc[(((Remove_duplicates_Merge_DF_new_df['min_branch'] == min_brch) & (Remove_duplicates_Merge_DF_new_df['max_branch'] == max_brch) & (Remove_duplicates_Merge_DF_new_df['treelevel'] == min_tree) & (Remove_duplicates_Merge_DF_new_df['order_num'] == max_ornum)), ['circuitid', 'opnum', 'devicetype', 'circuitname', 'min_branch', 'max_branch', 'treelevel', 'order_num', 'substationname', 'consider', 'operable', 'OPERABLE_TRANS_PMETER_COUNT', 'STATUS'])])
                                                 log.info("update_Remove_duplicates_Merge_DF_new_df is updated")
                                                 # print (update_Remove_duplicates_Merge_DF_new_df)
                                                 Remove_duplicates_Merge_DF_new_df = update_Remove_duplicates_Merge_DF_new_df
                                             new_df = pd.DataFrame(new_df.loc[((new_df['min_branch'] > new_min_value),
                                                                                   ['circuitid', 'opnum', 'devicetype',
                                                                                    'circuitname', 'min_branch',
                                                                                    'max_branch', 'treelevel', 'order_num',
                                                                                    'substationname', 'consider',
                                                                                    'operable',
                                                                                    'OPERABLE_TRANS_PMETER_COUNT',
                                                                                    'STATUS'])])

                                             new_df_sorted = new_df.sort_values(by=['min_branch'])
                                             new_df = new_df_sorted
                                             log.info("New sorted DF is obtained")
                                             Remove_duplicates_Merge_DF = pd.concat([Remove_duplicates_Merge_DF, Remove_duplicates_Merge_DF_new_df])
                                             Remove_duplicates_Merge_DF = pd.DataFrame(Remove_duplicates_Merge_DF.drop_duplicates())
                                             Merge_DF_sort_min_branch = Remove_duplicates_Merge_DF.sort_values(by=['min_branch'])
                                             # print(Merge_DF_sort_min_branch.head(10))
                                             # print(Merge_DF_sort_min_branch.shape[0])
                                             log.info("Merge_DF_sort_min_branch is updated")
                                             if not new_min_value in sorted_min_branch:
                                                 sorted_min_branch.append(new_min_value)
                                                 sorted_min_branch.sort()
                                                 sorted_max_branch.append(new_max_value)
                                                 sorted_max_branch.sort()
                                             break
                                         break
                         #update size info here
                         length = Remove_duplicates_Merge_DF.shape[0]
                         m += 1
        # write csv files
                 log.info("Final Merge_DF_sort_min_branch Dataframe is obtained")
                 # print (Merge_DF_sort_min_branch)
                 with open(verify_output_file, 'a') as f:
                     Merge_DF_sort_min_branch.to_csv(f, header=False)

        output_file_ND = downloadsfolderPath + "\\ResultFile"
        verify_output_file_ND = output_file_ND + "/" + "outfile.csv"
        deleteFolder(output_file_ND)
        create_folder(output_file_ND)
        New_remove_duplicates_df = pd.read_csv(verify_output_file)
        Remove_duplicates_df = pd.DataFrame(New_remove_duplicates_df.drop_duplicates())
        Remove_duplicates_df.to_csv(verify_output_file_ND, header= ['ObjectID', 'circuitid', 'opnum', 'devicetype', 'circuitname', 'min_branch',
                                                        'max_branch', 'treelevel', 'order_num', 'substationname',
                                                        'operable', 'consider', 'OPERABLE_TRANS_PMETER_COUNT',
                                                        'STATUS'], index= False)

        print("file comp logic for result file")
        df_new_output_final_file = pd.read_csv(verify_output_file_ND)
        # remove CB circuits without duplicates
        df_new_output_final_file = pd.DataFrame(df_new_output_final_file.loc[
                                                    ((df_new_output_final_file['opnum'] != 'CB'), ['ObjectID', 'circuitid', 'opnum', 'devicetype',
                                                              'circuitname', 'min_branch',
                                                              'max_branch', 'treelevel', 'order_num',
                                                              'substationname', 'consider',
                                                              'operable',
                                                              'OPERABLE_TRANS_PMETER_COUNT',
                                                              'STATUS'])])
        deleteFolder(output_file_ND)
        create_folder(output_file_ND)
        # add CB ckts
        df_CB = pd.DataFrame(df_ckt_file.loc[((df_ckt_file['opnum'] == 'CB'), ['ObjectID', 'circuitid', 'opnum', 'devicetype',
                                                              'circuitname', 'min_branch',
                                                              'max_branch', 'treelevel', 'order_num',
                                                              'substationname', 'consider',
                                                              'operable',
                                                              'OPERABLE_TRANS_PMETER_COUNT',
                                                              'STATUS'])])

        df_new_output_final_file = pd.concat([df_new_output_final_file, df_CB])
        df_new_output_final_file = df_new_output_final_file.drop(columns=['STATUS'])
        df_new_output_final_file.to_csv(verify_output_file_ND, header= ['ObjectID', 'circuitid', 'opnum', 'devicetype', 'circuitname', 'min_branch',
                                                        'max_branch', 'treelevel', 'order_num', 'substationname',
                                                        'operable', 'consider', 'OPERABLE_TRANS_PMETER_COUNT'], index= False)

        log.info("Result File has all the actual final parent circuits generated")
        # compare 2 files
        log.info("Starting Comparing two files")
        df_ckt_final_file = df_ckt_final_file.drop(columns=['fireindex', 'operable', 'consider'])
        # df_ckt_final_file = df_ckt_final_file.dropna(axis=1, how='all')
        list_unique_ckts_expected = list(df_ckt_final_file['circuitid'].unique())
        #for each in df_ckt_final_file.groupby(["circuitid"], sort=True): print(each)

        df_new_output_final_file = df_new_output_final_file.drop(columns=['ObjectID', 'consider', 'operable', 'OPERABLE_TRANS_PMETER_COUNT'])
        list_unique_ckts_actual = list(df_new_output_final_file['circuitid'].unique())
        #for each in df_new_output_final_file.groupby(["circuitid"], sort=True): print (each)

        mismatch_file = downloadsfolderPath + "\\MismatchFile"
        mismatched_file = mismatch_file + "/" + "Mismatchedfile.csv"
        deleteFolder(mismatch_file)
        create_folder(mismatch_file)
        Mismatched_df = pd.DataFrame()
        # print (df_ckt_final_file.equals(df_new_output_final_file)

        for y in range ((len(list_unique_ckts_expected))-1):
            cmp_df_ckt_final_file_expected = df_ckt_final_file.loc[(df_ckt_final_file['circuitid'] == list_unique_ckts_expected[y])]
            cmp_df_ckt_final_file_expected = cmp_df_ckt_final_file_expected.sort_values(by=['min_branch'])
            cmp_df_new_output_final_file_actual = df_new_output_final_file.loc[(df_new_output_final_file['circuitid'] == list_unique_ckts_expected[y])]
            cmp_df_new_output_final_file_actual = cmp_df_new_output_final_file_actual.sort_values(by=['min_branch'])

            values_compare = cmp_df_ckt_final_file_expected.values == cmp_df_new_output_final_file_actual.values

            if not list_unique_ckts_expected[y] in list_unique_ckts_actual:
                # cmp_df_ckt_final_file_expected.iloc[item[0], item[1]] = ' {} --> {} '.format(cmp_df_ckt_final_file_expected.iloc[item[0], item[1]], cmp_df_new_output_final_file_actual.iloc[item[0], item[1]])
                cmp_df_ckt_final_file_expected = cmp_df_ckt_final_file_expected.loc[(cmp_df_ckt_final_file_expected['circuitid'] == list_unique_ckts_expected[y])]
                values = "Circuit Missing -->  " + str (list_unique_ckts_expected[y])
                cmp_df_ckt_final_file_expected = cmp_df_ckt_final_file_expected.replace(to_replace=list_unique_ckts_expected[y], value= values)
                Mismatched_df = Mismatched_df.append(cmp_df_ckt_final_file_expected)
            rows, cols = np.where(values_compare == False)
            for item in zip(rows, cols):
                cmp_df_ckt_final_file_expected.iloc[item[0], item[1]] = ' {} --> {} '.format(cmp_df_ckt_final_file_expected.iloc[item[0], item[1]], cmp_df_new_output_final_file_actual.iloc[item[0], item[1]])
                Mismatched_df = Mismatched_df.append(cmp_df_ckt_final_file_expected)

        Mismatched_df.to_csv(mismatched_file, header=['circuitid', 'opnum', 'devicetype', 'circuitname',
                                                'min_branch', 'max_branch', 'treelevel', 'order_num', 'substationname'], index=False)

         # Remove_duplicates_df_new = pd.read_csv(verify_output_file_ND)
        log.info("Completed Comparing two files and the mismatched data could be found in MismatchFile folder in downloads folder")
        log.info("----------------------------------------------------------------------------------------------")
        log.info("*************AUTOMATION EXECUTION COMPLETED*************")
