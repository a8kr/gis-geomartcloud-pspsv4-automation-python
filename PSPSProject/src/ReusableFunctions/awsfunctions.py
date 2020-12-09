import os
import io
import zipfile
import boto3
import botocore
import pandas as pd


class AWSS3Bucket:
    """
        Support class for aws S3 bucket and file handling
    """

    def __init__(self, environment, profilename=''):
        """This function to initialize the member variables of this class.
        Args:
            self - current instance of this class
            environment - config environment
            profilename - aws profile name

        Returns:

        """
        try:
            self.environment = environment
            self.profilename = profilename

            if self.environment == "LOCAL":
                boto3.setup_default_session(profile_name=self.profilename)
                session = boto3.session.Session(profile_name=self.profilename)

            self.s3 = boto3.resource('s3')
            self.s3_conn = boto3.client('s3')

        except Exception as e:
            raise Exception("awsS3bucket initialization failed.." + str(e))

    def getfilenamesinbucket(self, bucket_name=None, folder_path=None, childfoldercontent=False):
        """This function is to check if file is locked.

            Args:
                bucket_name - the bucket name in s3
                folder_path - path contains subfolder and filename
                childfoldercontent - True/False if need to tranverse subfolders

            Returns:
                return filenames in dict
        """
        try:

            print('folderList - ' + folder_path)
            folders = []
            files = []
            result = dict()
            bucket_name = bucket_name
            prefix = folder_path

            # filter list for a given folder
            s3_result = self.s3_conn.list_objects_v2(Bucket=bucket_name, Delimiter="/", Prefix=prefix)

            if 'Contents' not in s3_result and 'CommonPrefixes' not in s3_result:
                return []

            # Get folder names in S3 bucket
            if s3_result.get('CommonPrefixes'):
                for folder in s3_result['CommonPrefixes']:
                    folders.append(folder.get('Prefix'))

            # Get files in S3 bucket
            if s3_result.get('Contents'):
                for key in s3_result['Contents']:
                    files.append(key['Key'])

            # Traverse files through complete list
            while s3_result['IsTruncated']:
                continuation_key = s3_result['NextContinuationToken']
                s3_result = self.s3_conn.list_objects_v2(Bucket=bucket_name, Delimiter="/",
                                                         ContinuationToken=continuation_key, Prefix=prefix)

                if s3_result.get('CommonPrefixes'):
                    for folder in s3_result['CommonPrefixes']:
                        folders.append(folder.get('Prefix'))

                if s3_result.get('Contents'):
                    for key in s3_result['Contents']:
                        files.append(key['Key'])

            # Traverse through all folder in a given path
            if childfoldercontent:
                for folder in folders:
                    res = self.getfilenamesinbucket(bucket_name, folder, childfoldercontent)

                    if (res != None):
                        if "files" in res:
                            files.extend(res["files"])

                            # list output
            if folders:
                result['folders'] = sorted(folders)
            if files:
                result['files'] = sorted(files)

            return result
        except Exception as e:
            print(e)
            raise Exception("Get S3 bucket file failed " + str(e))

    def movetos3bucket(self, sourcebucketname, sourcepath, destbucketname, destpath, childfoldercontent=False):
        """This function is to check if file is locked.

            Args:
                sourcebucketname - the bucket name in s3
                sourcepath - path contains subfolder and filename
                destbucketname - destiny bucket name
                destpath - path contains destiny subfolder and filename
                childfoldercontent - True/False if need to tranverse subfolders

            Returns:
                return True if file moved successfully
        """
        try:
            print('copy child - ' + str(childfoldercontent))

            res = self.getfilenamesinbucket(sourcebucketname, sourcepath, childfoldercontent)

            print('Files to move - ' + str(len(res["files"])))
            count = 0
            subcount = 0

            for file in res["files"]:
                filename, file_extension = os.path.splitext(file)

                if (len(file_extension) > 0):
                    copy_source = {'Bucket': sourcebucketname, 'Key': file}
                    destkey = str.replace(file, sourcepath, destpath)

                    self.s3.meta.client.copy(copy_source, destbucketname, destkey)

                    count = count + 1
                    subcount = subcount + 1

                    if subcount == 50:
                        print(".", end=" ")

                    if subcount == 200:
                        subcount = 0
                        print(str(count), end=" ")

            print("File copy completed successfully")

            return True
        except Exception as e:
            print(e)
            return False

    def deletefolder(self, bucketname, folderpath):
        """This function is to check if file is locked.

            Args:
                bucketname - the bucket name in s3
                folderpath - path contains subfolder and filename
            Returns:
                return True if file moved successfully
        """
        try:

            print('Delete the folder at - ' + folderpath)

            if self.checkkeyexist(bucketname, folderpath):
                bucket = self.s3.Bucket(bucketname)
                bucket.objects.filter(Prefix=folderpath).delete()

            return True
        except Exception as e:
            print(e)
            return False

    def downloadfile(self, bucketName, bucketFullPath):
        """This function is to download the object of file from s3 bucket.

            Args:
                bucketname - the bucket name in s3
                bucketFullPath - path contains subfolder and filename
            Returns:
                return object of file, if no file then return None.
        """
        try:
            obj = self.s3_conn.get_object(Bucket=bucketName, Key=bucketFullPath)
            return obj['Body']
        except botocore.exceptions.ClientError as e:
            if (e.response["Error"]["Code"] == "NoSuchKey"):
                print(" file does not exist in s3")
            return None
        except Exception as e:
            print(e)
            raise Exception("downloadfile failed.." + str(e))

    def downloadfile_local(self, bucketName, bucketFullPath, localInputFilePath):
        """This function is to download the file from s3 bucket.

            Args:
                bucketname - the bucket name in s3
                bucketFullPath - path contains subfolder and filename
            Returns:
                return True if file download successfully else return false.
        """
        try:
            self.s3.Bucket(bucketName).download_file(bucketFullPath, localInputFilePath)
            return True
        except botocore.exceptions.ClientError as e:
            if (e.response["Error"]["Code"] == "NoSuchKey"):
                print(" file does not exist in s3")
            return False
        except Exception as e:
            print(e)
            raise Exception("downloadfile failed.." + str(e))

    def fileSize_file(self, bucketName, bucketFullPath):
        """This function is to return the file size of s3 bucket file.

            Args:
                bucketname - the bucket name in s3
                bucketFullPath - path contains subfolder and filename
            Returns:
                return length of file, if no file size then return None.
        """
        try:
            length = self.s3.Bucket(bucketName).Object(bucketFullPath).content_length
            return length
        except botocore.exceptions.ClientError as e:
            if (e.response["Error"]["Code"] == "NoSuchKey"):
                print(" file does not exist in s3")
            return None
        except Exception as e:
            print(e)
            raise Exception("fileSize_file failed.." + str(e))

    def upload_report_toS3(self, local_filepath, s3_bucketname, s3_filepath):
        """"This function is to upload file into s3 bucket

            Args:
                local_filepath - local file path
                s3_bucketname - s3 bucket name
                s3_filepath - s3 bucket path

            Returns: True if success otherwise return false

        """
        try:
            self.s3_conn.upload_file(local_filepath, s3_bucketname, s3_filepath)
            return True
        except Exception as e:
            print(e)
            return False

    def upload_report(self, output_bucketpath, output_report_bucketname, output_filename, csv_buffer):
        """"This function is to upload file into s3 bucket

                Args:
                    output_bucketpath - bucket path name
                    output_report_bucketname - report bucket name
                    output_filename - target file name
                    csv_buffer - buffer object of csv
                Returns: True if success otherwise return false

        """
        try:
            print(output_bucketpath)
            # create folder in s3Bucket
            self.s3.Object(
                output_report_bucketname, output_bucketpath + "/").load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                print('Folder does not exists. create folder')
                self.s3.Object(
                    output_report_bucketname, output_bucketpath + "/").put(Body='')
            else:
                print('folder exists')
        try:
            # get the file name for common report
            filename = output_bucketpath + "/" + output_filename
            print('bucketname: ' + str(output_report_bucketname))

            # Write buffer to S3 object
            self.s3.Object(output_report_bucketname, filename).put(
                Body=csv_buffer.getvalue())
            print('file saved to s3: ' + str(filename))
            return True
        except Exception as e:
            print(e)
            return False

    def checkkeyexist(self, bucketname, keypath, childfoldercontent=False):
        """This function is to check if file is locked.

            Args:
                bucketname - the bucket name in s3
                keypath - path contains subfolder and filename
                childfoldercontent - True/False if need to tranverse subfolders
            Returns:
                return True if file exists
        """
        try:
            res = self.getfilenamesinbucket(bucketname, keypath, childfoldercontent)

            if len(res) > 0:
                if "files" in res:
                    if len(res["files"]) > 0:
                        return True

                if "folders" in res:
                    if len(res["folders"]) > 0:
                        return True

            return False
        except Exception as e:
            print(e)
            raise Exception("checkkeyexist exist in bucket " + bucketname + "  Failed - " + str(e))

    def unzipfile(self, frombucket, frompath, tobucket, topath):
        """This function is to check if file is locked.

            Args:
                frombucket - the bucket name in s3
                frompath - path contains subfolder and filename
                tobucket - distiny bucket name in s3
                topath - path contains distiny subfolder and filename
            Returns:
                return True if file unzipped
        """
        try:
            if not self.checkkeyexist(frombucket, frompath):
                print("Error: From bucket key info does not exist")
                return False

            # get file from s3 bucket
            zip_obj = self.s3.Object(bucket_name=frombucket, key=frompath)

            # read zip file
            buffer = io.BytesIO(zip_obj.get()["Body"].read())

            z = zipfile.ZipFile(buffer)

            for filename in z.namelist():
                file_info = z.getinfo(filename)

                # get file from zip and upload to bucket
                self.s3.meta.client.upload_fileobj(
                    z.open(filename),
                    Bucket=tobucket,
                    Key=topath + filename  # f'{filename}'
                )

            print("unzip " + frompath + " completed")

            return True
        except Exception as e:
            print(e)
            return False


def read_file_from_S3(awsinputbucketname, fileName, awsactivetablebucketpath, profilename):
    try:
        obj_key = awsactivetablebucketpath + fileName
        file_history = []

        boto3.setup_default_session(profile_name=profilename)
        s3 = boto3.client('s3')

        files = s3.list_objects_v2(Bucket=awsinputbucketname)['Contents']
        for file in files:
            if obj_key in file['Key']:
                file_history.append(file)
        latestfile = lambda obj: int(obj['LastModified'].timestamp())
        filepath = [obj['Key'] for obj in sorted(file_history, key=latestfile, reverse=True)][0]

        obj = s3.get_object(Bucket=awsinputbucketname, Key=filepath)
        data = pd.read_csv(obj['Body'])
        return data

    except botocore.exceptions.ClientError as e:
        if (e.response["Error"]["Code"] == "NoSuchKey"):
            print(fileName + " file does not exist in s3")


def created_updated_times3bucket(awsinputbucketname, fileName, awsactivetablebucketpath):
    try:
        obj_key = awsactivetablebucketpath + fileName
        boto3.setup_default_session(profile_name='GISCOE-Dev')
        s3 = boto3.client('s3')
        metadata = s3.head_object(Bucket=awsinputbucketname, Key=obj_key)
        print(dict(metadata))
        created_date = metadata['ResponseMetadata']['HTTPHeaders']['date']
        last_modified = metadata['ResponseMetadata']['HTTPHeaders']['last-modified']
        return [created_date, last_modified]
    except botocore.exceptions.ClientError as e:
        if (e.response["Error"]["Code"] == "NoSuchKey"):
            print(fileName + "file does not exist in s3")
            return [None, None]
        return [None, None]


def download_file_from_S3(awsinputbucketname, awsactivetablebucketpath, fileName, LOCAL_FILE_NAME, profile_name):
    try:
        obj_key = awsactivetablebucketpath + fileName
        boto3.setup_default_session(profile_name=profile_name)
        s3 = boto3.client('s3')
        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket(awsinputbucketname)
        s3.download_file(awsinputbucketname, obj_key, LOCAL_FILE_NAME)
    except botocore.exceptions.ClientError as e:
        if (e.response["Error"]["Code"] == "NoSuchKey"):
            print(fileName + "file does not exist in s3")


def download_dir_from_S3(dist, bucket,profile_name, local='/tmp'):
    boto3.setup_default_session(profile_name=profile_name)
    client = boto3.client('s3')
    resource = boto3.resource('s3')
    paginator = client.get_paginator('list_objects')
    for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist):
        if result.get('CommonPrefixes') is not None:
            for subdir in result.get('CommonPrefixes'):
                download_dir_from_S3(subdir.get('Prefix'), local, bucket)
        for file in result.get('Contents', []):
            # dest_pathname = os.path.join(local, file.get('Key'))
            dest_pathname = os.path.join(local, file.get('Key'))
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
            resource.meta.client.download_file(bucket, file.get('Key'), dest_pathname)