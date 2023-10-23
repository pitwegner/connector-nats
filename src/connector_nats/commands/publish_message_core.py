"""Publish message on a NATS core subject."""
import nats

class PublishMessageCore:
    """Publish message on a NATS core subject."""

    def __init__(self, endpoint, username, password, subject, message):
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

    async def execute(self, _config, _task_data):
        status = 0
        error = None
        try:
            nc = await nats.connect(
                self.endpoint,
                user=self.username,
                password=self.password
            )
            await nc.publish(self.subject, self.message.encode())
            status = 200
        except Exception as exception:
            error = {"error_code": exception.__class__.__name__, "message": str(exception)}
            status = 500

        return_response = {
            "body": "{}",
            "mimetype": "application/json",
            "http_status": status,
        }
        result = {
            "command_response": return_response,
            "error": error,
            "command_response_version": 2,

        }
        return result
