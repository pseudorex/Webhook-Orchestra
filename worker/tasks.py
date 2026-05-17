import requests

from celery.exceptions import MaxRetriesExceededError

from worker.celery_app import celery


@celery.task(
    bind=True,
    max_retries=3
)
def deliver_webhook(
    self,
    webhook_url,
    payload
):

    try:

        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10
        )

        response.raise_for_status()

        print("=================================")
        print("WEBHOOK DELIVERED")
        print("STATUS:", response.status_code)
        print("=================================")

    except Exception as e:

        print("=================================")
        print("WEBHOOK FAILED")
        print(str(e))
        print("=================================")

        countdown = 2 ** self.request.retries

        if self.request.retries >= self.max_retries:

            print("=================================")
            print("MAX RETRIES EXCEEDED")
            print("EVENT MOVED TO DEAD STATE")
            print("=================================")

            return

        raise self.retry(
            exc=e,
            countdown=countdown
        )