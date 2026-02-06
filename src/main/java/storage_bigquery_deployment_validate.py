from result_output import *
import sys
import json
import importlib.util
from googleapiclient import discovery
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
from googleapiclient.errors import HttpError

class Activity():

    # Testcase 1: Check Cloud Storage Bucket
    def testcase_check_cloud_storage_bucket(self, test_object, credentials, project_id):
        testcase_description = "Verify app-storage-bucket exists"
        expected_result = "app-storage-bucket"
        marks = 10

        try:
            is_present = False
            actual = 'Bucket name is not ' + expected_result
            storage_client = storage.Client(credentials=credentials)

            try:
                buckets = storage_client.list_buckets()
                for bucket in buckets:
                    if bucket.name == expected_result:
                        is_present = True
                        actual = expected_result
                        break
            except Exception as e:
                is_present = False

            test_object.update_pre_result(testcase_description, expected_result)
            if is_present:
                test_object.update_result(1, expected_result, actual,
                    "Congrats! You have done it right!", " ", marks)
            else:
                test_object.update_result(0, expected_result, actual,
                    "Check Cloud Storage Bucket name",
                    "https://cloud.google.com/storage/docs/creating-buckets", marks)
        except Exception as e:
            test_object.update_result(-1, expected_result, "Internal Server error",
                "Please check with Admin", "", marks)
            test_object.eval_message["testcase_check_cloud_storage_bucket"] = str(e)

    # Testcase 2: Check Cloud Storage Region
    def testcase_check_cloud_storage_region(self, test_object, credentials, project_id):
        testcase_description = "Verify bucket is created in us-central1"
        expected_result = "us-central1"
        marks = 5

        try:
            is_correct_region = False
            actual = 'Bucket location is not ' + expected_result
            storage_client = storage.Client(credentials=credentials)

            try:
                buckets = storage_client.list_buckets()
                for bucket in buckets:
                    if bucket.name == "app-storage-bucket" and bucket.location == expected_result:
                        is_correct_region = True
                        actual = expected_result
                        break
            except Exception as e:
                is_correct_region = False

            test_object.update_pre_result(testcase_description, expected_result)
            if is_correct_region:
                test_object.update_result(1, expected_result, actual,
                    "Congrats! You have done it right!", " ", marks)
            else:
                test_object.update_result(0, expected_result, actual,
                    "Check Cloud Storage Bucket location",
                    "https://cloud.google.com/storage/docs/locations", marks)
        except Exception as e:
            test_object.update_result(-1, expected_result, "Internal Server error",
                "Please check with Admin", "", marks)
            test_object.eval_message["testcase_check_cloud_storage_region"] = str(e)

    # Testcase 3: Check Bucket Versioning
    def testcase_check_bucket_versioning(self, test_object, credentials, project_id):
        testcase_description = "Verify versioning is enabled on the bucket"
        expected_result = "Versioning is enabled"
        marks = 5

        try:
            is_versioning_enabled = False
            actual = 'Versioning is not enabled'
            storage_client = storage.Client(credentials=credentials)

            try:
                buckets = storage_client.list_buckets()
                for bucket in buckets:
                    if bucket.name == "app-storage-bucket" and bucket.versioning_enabled:
                        is_versioning_enabled = True
                        actual = expected_result
                        break
            except Exception as e:
                is_versioning_enabled = False

            test_object.update_pre_result(testcase_description, expected_result)
            if is_versioning_enabled:
                test_object.update_result(1, expected_result, actual,
                    "Congrats! You have done it right!", " ", marks)
            else:
                test_object.update_result(0, expected_result, actual,
                    "Check Cloud Storage Bucket versioning",
                    "https://cloud.google.com/storage/docs/object-versioning", marks)
        except Exception as e:
            test_object.update_result(-1, expected_result, "Internal Server error",
                "Please check with Admin", "", marks)
            test_object.eval_message["testcase_check_bucket_versioning"] = str(e)

    # Testcase 4: Check BigQuery Dataset
    def testcase_check_bigquery_dataset(self, test_object, credentials, project_id):
        testcase_description = "Verify analytics_dataset exists"
        expected_result = "analytics_dataset"
        marks = 10

        try:
            is_present = False
            actual = 'Dataset name is not ' + expected_result
            bigquery_client = bigquery.Client(credentials=credentials)

            try:
                datasets = bigquery_client.list_datasets(project=project_id)
                for dataset in datasets:
                    if dataset.dataset_id == expected_result:
                        is_present = True
                        actual = expected_result
                        break
            except Exception as e:
                is_present = False

            test_object.update_pre_result(testcase_description, expected_result)
            if is_present:
                test_object.update_result(1, expected_result, actual,
                    "Congrats! You have done it right!", " ", marks)
            else:
                test_object.update_result(0, expected_result, actual,
                    "Check BigQuery Dataset name",
                    "https://cloud.google.com/bigquery/docs/datasets", marks)
        except Exception as e:
            test_object.update_result(-1, expected_result, "Internal Server error",
                "Please check with Admin", "", marks)
            test_object.eval_message["testcase_check_bigquery_dataset"] = str(e)

    # Testcase 5: Check BigQuery Table
    def testcase_check_bigquery_table(self, test_object, credentials, project_id):
        testcase_description = "Verify user_data table exists in the dataset"
        expected_result = "user_data"
        marks = 10

        try:
            is_present = False
            actual = 'Table name is not ' + expected_result
            bigquery_client = bigquery.Client(credentials=credentials)

            try:
                dataset_ref = bigquery_client.dataset("analytics_dataset")
                tables = bigquery_client.list_tables(dataset_ref)
                for table in tables:
                    if table.table_id == expected_result:
                        is_present = True
                        actual = expected_result
                        break
            except Exception as e:
                is_present = False

            test_object.update_pre_result(testcase_description, expected_result)
            if is_present:
                test_object.update_result(1, expected_result, actual,
                    "Congrats! You have done it right!", " ", marks)
            else:
                test_object.update_result(0, expected_result, actual,
                    "Check BigQuery Table name",
                    "https://cloud.google.com/bigquery/docs/tables", marks)
        except Exception as e:
            test_object.update_result(-1, expected_result, "Internal Server error",
                "Please check with Admin", "", marks)
            test_object.eval_message["testcase_check_bigquery_table"] = str(e)

def start_tests(credentials, project_id, args):
    if "result_output" not in sys.modules:
        importlib.import_module("result_output")
    else:
        importlib.reload(sys.modules["result_output"])

    test_object = ResultOutput(args, Activity)
    challenge_test = Activity()

    # Execute all testcases
    challenge_test.testcase_check_cloud_storage_bucket(test_object, credentials, project_id)
    challenge_test.testcase_check_cloud_storage_region(test_object, credentials, project_id)
    challenge_test.testcase_check_bucket_versioning(test_object, credentials, project_id)
    challenge_test.testcase_check_bigquery_dataset(test_object, credentials, project_id)
    challenge_test.testcase_check_bigquery_table(test_object, credentials, project_id)

    json.dumps(test_object.result_final(), indent=4)
    return test_object.result_final()