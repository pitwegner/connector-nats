"""Publish message on a NATS core subject."""
import nats
import asyncio


class PublishMessageCore:
    """Publish message on a NATS core subject."""

    def __init__(self, endpoint, subject, message, username=None, password=None):
        """
        :param endpoint: The endpoint to use.
        :param username: The username to authenticate with.
        :param password: The password to authenticate with.
        :param subject: The subject which the message should be published to.
        :param message: The message payload.
        :return:
        """
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.subject = subject
        self.message = message

    def execute(self, _config, _task_data):
        return asyncio.run(self.async_exec())

    async def async_exec(self):
        status = 0
        try:
            nc = await nats.connect(
                self.endpoint,
                user=self.username,
                password=self.password
            )
            await nc.publish(self.subject, self.message.encode())
            status = 200
        except Exception as exception:
            return {
                "response": {"error": str(exception)},
                "mimetype": "application/json",
                "status": 500,
            }

        return {
            "response": {},
            "mimetype": "application/json",
            "status": status,
        }
