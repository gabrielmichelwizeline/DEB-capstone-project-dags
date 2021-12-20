from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.decorators.track_status import track_operator_status


class S3ListKeys(BaseOperator):

    template_fields = ()

    template_ext = ()

    ui_color = "#ededed"

    @apply_defaults
    def __init__(
        self,
        bucket_name,
        prefix="",
        aws_conn_id="aws_default",
        verify=None,
        max_items=None,
        page_size=None,
        *args,
        **kwargs
    ):
        super(S3ListKeys, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.max_items = max_items
        self.page_size = page_size
        self.track_status = kwargs.get("track_status") or False

    @track_operator_status
    def execute(self, context):
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        self.log.info("Downloading S3 keys")
        keys = self.s3.list_keys(
            self.bucket_name,
            prefix=self.prefix,
            max_items=self.max_items,
            page_size=self.page_size,
        )
        keys = [k for k in keys if k != self.prefix]
        context["ti"].xcom_push(key="s3_keys", value=keys)
