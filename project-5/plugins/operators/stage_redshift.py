from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    r"""
    Copies data from an S3 bucket and stages it in Redshift.

    :param redshift_conn_id: the ID of the Redshift connection
    :param aws_credentials_id: the ID of the AWS credentials to be used
    :param table: the table to populate
    :param s3_bucket: the bucket that holds the data
    :param s3_key: the S3 key to the file that holds the data
    :param file_type: json or csv
    :param json_path: for JSON files, the path file that describe the data, defaults to "auto"
    :param delimiter: CSV file delimiter
    :param ignore_headers: should the header be skipped for a CSV
    """
    ui_color = '#358140'
    
    copy_sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}'
    """
    
    copy_sql_csv = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="",
                s3_key="",
                file_type="json",
                json_path="auto",
                delimiter=",",
                ignore_headers=1,
                *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_type = file_type
        self.json_path = json_path
        self.aws_credentials_id = aws_credentials_id
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        if self.file_type == "json":
            formatted_sql = StageToRedshiftOperator.copy_sql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql_csv.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,                
                self.delimiter
            )

        self.log.info(f"Copying data from {s3_path} to Redshift")
        redshift.run(formatted_sql)        
