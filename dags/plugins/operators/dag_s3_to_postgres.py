import io
import os.path

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.decorators.track_status import track_operator_status


class S3ToPostgresTransferFromXcom(BaseOperator):

    template_fields = ()

    template_ext = ()

    ui_color = "#ededed"

    @apply_defaults
    def __init__(
        self,
        schema,
        table,
        bucket_name,
        pull_from_tasks=None,
        aws_conn_postgres_id="postgres_default",
        aws_conn_id="aws_default",
        verify=None,
        copy_options=tuple(),
        autocommit=False,
        parameters=None,
        move_file=True,
        dest_prefix=None,
        track_status=False,
        *args,
        **kwargs
    ):
        super(S3ToPostgresTransferFromXcom, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.bucket_name = bucket_name
        self.pull_from_tasks = pull_from_tasks or []
        self.aws_conn_postgres_id = aws_conn_postgres_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters
        self.move_file = move_file
        self.dest_prefix = dest_prefix
        if not dest_prefix:
            self.dest_prefix = None
            self.move_file = False
        self.track_status = track_status

    @track_operator_status
    def execute(self, context):

        self.log.info(self.aws_conn_postgres_id)

        self.pg_hook = PostgresHook(postgre_conn_id=self.aws_conn_postgres_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        self.s3_key_data = context["ti"].xcom_pull(
            key="s3_keys",
        )

        if not self.s3_key_data:
            return

        for key in self.s3_key_data:
            self.current_key = key

            self.log.info("Downloading S3 file")
            self.log.info(self.current_key + ", " + self.bucket_name)

            if not self.s3.check_for_key(self.current_key, self.bucket_name):
                raise AirflowException(
                    "The key {0} does not exist".format(self.current_key)
                )

            s3_key_object = self.s3.get_key(self.current_key, self.bucket_name)
            print(s3_key_object)

            response = (
                s3_key_object.get()["Body"]
                .read()
                .decode(encoding="utf-8", errors="ignore")
            )
            schema = {
                "invoice_number": "string",
                "stock_code": "string",
                "detail": "string",
                "quantity": "string",
                "invoice_date": "string",
                "unit_price": "string",
                "customer_id": "string",
                "country": "string",
            }

            df_products = pd.read_csv(
                io.StringIO(response),
                header=0,
                delimiter=",",
                quotechar='"',
                low_memory=False,
                dtype=schema,
            )
            self.log.info(df_products)
            self.log.info(df_products.info())

            df_products = df_products.replace(r"[\"]", r"'")
            list_df_products = df_products.values.tolist()
            list_df_products = [tuple(x) for x in list_df_products]
            self.log.info(list_df_products)

            # Setup database and table
            file_name = "bootcampdb.user_purchase.sql"

            file_path = (
                "/usr/local/airflow/operators/assets" + os.path.sep + file_name
            )
            self.log.info(file_path)
            file_mode = "r"
            encoding = "UTF-8"

            with open(file_path, file_mode, encoding=encoding) as file_manipulator:

                # Read file with the DDL CREATE TABLE
                SQL_COMMAND_CREATE_TBL = file_manipulator.read()

                # Display the content
                self.log.info(SQL_COMMAND_CREATE_TBL)

            # # DB insert
            # self.pg_hook.run(SQL_COMMAND_CREATE_TBL)

            # list_target_fields = [
            #     "invoice_number",
            #     "stock_code",
            #     "detail",
            #     "quantity",
            #     "invoice_date",
            #     "unit_price",
            #     "customer_id",
            #     "country",
            # ]

            # self.current_table = self.schema + "." + self.table
            # self.pg_hook.insert_rows(
            #     self.current_table,
            #     list_df_products,
            #     target_fields=list_target_fields,
            #     commit_every=1000,
            #     replace=False,
            # )

            self.log.info("Rows inserted!")

            if self.move_file:
                dest_key = self.dest_prefix + self.current_key.split("/")[-1]
                s3_key_object = self.s3.copy_object(
                    self.current_key,
                    dest_key,
                    dest_bucket_name=self.bucket_name,
                    source_bucket_name=self.bucket_name,
                )

            self.s3.delete_objects(self.bucket_name, self.current_key)

            self.log.info("Object moved to processed folder!")
