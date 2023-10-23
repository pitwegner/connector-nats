"""Publish message on a NATS jetstream subject."""
import json
import nats


class PublishMessageJetStream:
    """Publish message on a NATS jetstream subject."""

    def __init__(self, endpoint, subject, stream, message, username=None, password=None):
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
        self.stream = stream
        self.message = message

    async def execute(self, _config, _task_data):
        command_response = {}
        status = 0
        error = None
        try:
            nc = await nats.connect(
                self.endpoint,
                user=self.username,
                password=self.password
            )
            js = nc.jetstream()
            await js.add_stream(name=self.stream, subjects=[self.subject])
            ack = await js.publish(self.subject, self.message.encode())
            command_response = ack.as_dict()
            status = 200
        except Exception as exception:
            error = {"error_code": exception.__class__.__name__, "message": str(exception)}
            status = 500

        return_response = {
            "body": json.dumps(command_response),
            "mimetype": "application/json",
            "http_status": status,
        }
        result = {
            "command_response": return_response,
            "error": error,
            "command_response_version": 2,

        }
        return result
