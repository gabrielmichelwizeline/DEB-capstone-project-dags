from functools import wraps


def track_operator_status(excecute):
    @wraps(excecute)
    def wrapper(*args, **kwargs):
        try:
            self = args[0]
            context = kwargs["context"]
            excecute(*args, **kwargs)
            if self.track_status:
                context["ti"].xcom_push(value=context["ti"].task_id, key="success")
        except Exception as e:
            if self.track_status:
                context["ti"].xcom_push(value=context["ti"].task_id, key="fail")
            self.log.info(f"Caused by exception: {e}")
            raise e

    return wrapper
