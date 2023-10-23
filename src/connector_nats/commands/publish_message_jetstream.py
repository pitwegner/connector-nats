"""Publish message on a NATS jetstream subject."""
import json
from typing import Any
import nats

from spiffworkflow_connector_command.command_interface import CommandErrorDict
from spiffworkflow_connector_command.command_interface import CommandResponseDict
from spiffworkflow_connector_command.command_interface import ConnectorCommand
from spiffworkflow_connector_command.command_interface import ConnectorProxyResponseDict


class PublishMessageJetStream(ConnectorCommand):
    """Publish message on a NATS jetstream subject."""

    def __init__(self, endpoint: str, username: str, password: str,
                 subject: str, stream: str, message: str):
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

    async def execute(self, _config: Any, _task_data: Any) -> ConnectorProxyResponseDict:
        command_response = {}
        status = 0
        error: CommandErrorDict | None = None
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

        return_response: CommandResponseDict = {
            "body": json.dumps(command_response),
            "mimetype": "application/json",
            "http_status": status,
        }
        result: ConnectorProxyResponseDict = {
            "command_response": return_response,
            "error": error,
            "command_response_version": 2,

        }
        return result
