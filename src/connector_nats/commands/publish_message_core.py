"""Publish message on a NATS core subject."""
from typing import Any
import nats

from spiffworkflow_connector_command.command_interface import CommandErrorDict
from spiffworkflow_connector_command.command_interface import CommandResponseDict
from spiffworkflow_connector_command.command_interface import ConnectorCommand
from spiffworkflow_connector_command.command_interface import ConnectorProxyResponseDict


class PublishMessageCore(ConnectorCommand):
    """Publish message on a NATS core subject."""

    def __init__(self, endpoint: str, username: str, password: str,
                 subject: str, message: str):
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

    async def execute(self, _config: Any, _task_data: Any) -> ConnectorProxyResponseDict:
        status = 0
        error: CommandErrorDict | None = None
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

        return_response: CommandResponseDict = {
            "body": "{}",
            "mimetype": "application/json",
            "http_status": status,
        }
        result: ConnectorProxyResponseDict = {
            "command_response": return_response,
            "error": error,
            "command_response_version": 2,

        }
        return result
