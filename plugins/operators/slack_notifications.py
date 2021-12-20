import json

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.utils.decorators import apply_defaults


class NotifySlackDagResult(SlackWebhookOperator):
    @apply_defaults
    def __init__(self, message_success=None, message_failure=None, *args, **kwargs):
        super(NotifySlackDagResult, self).__init__(*args, **kwargs)
        self.message_success = message_success or "Success"
        self.message_failure = message_failure or "Failure"

    def execute(self, context):
        task_ids, _ = zip(*context["dag"].task_dict.items())
        report_dict = {}
        all_statuses = []

        for status in ["success", "fail"]:
            results = context["ti"].xcom_pull(key=status, task_ids=task_ids[:-1])
            clean_results = [r for r in results if r]
            report_dict[status.capitalize()] = clean_results
            all_statuses.extend(clean_results)

        untracked_tasks = list(set(task_ids[:-1]).difference(all_statuses))
        report_dict["Untracked"] = untracked_tasks

        self.message = "A DAG has finished running!"
        formatted_tasks = '• ' + "\n• ".join(task_ids)

        print(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{formatted_tasks}",
                    "emoji": True,
                },
            }
        )
        self.blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "A *DAG* has finished running!\nThe tasks were run in this order:",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{formatted_tasks}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{json.dumps(report_dict, indent=2)}```",
                },
            },
        ]

        super(NotifySlackDagResult, self).execute(context)
