import json
import logging
import requests
from typing import Any
from configs.config import config
from workers.base_worker import BaseWorker

IMPORT_BATCH_SIZE = 51

class ImportWorker(BaseWorker):
    config = {}
    def __init__(self) -> None:
        super().__init__()
        self.exchangeName = 'import_tool'
        self.queueName = 'import_tool'
        self.validate_url = config['duchampUrl'] + '/Importv2Endpoints/validate_row'
        self.process_url = config['duchampUrl'] + '/Importv2Endpoints/process_row'
        self.session = requests.Session()
        self.command_map = {
            'validate': 'validation',
            'process': 'processing'
        }
        self.connectToDB()

    def connectToDB(self):
        """
        Initializes the database models used by the ImportWorker class.
        """
        from models.apps_db import Import, ImportRow, ImportValidationError
        self.Import = Import()
        self.ImportRow = ImportRow()
        self.ImportValidationError = ImportValidationError()

    def runWorker(self, ch, method, properties, body):
        self.heartbeat()
        try:
            params = json.loads(body.decode('utf-8'))
            import_id = params.get('import_id')
            import_command = params.get('import_command')
            if import_id is None or import_command is None:
                logging.info(f'invalid message: {params}')
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return False
            result = self.handle_import_command(import_id, import_command)
            if result == 'invalid command':
                logging.info(f'invalid input command: {params}')
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return False
            logging.info(f'completed params: {params}, result: {result}')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return True
        except Exception as e:
            logging.exception(str(e))
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return True

    def handle_import_command(self, import_id: str, import_command: str) -> str:
        """
        Map the given import command to a step in the import process and perform that step.
        Args:
            import_id (str): The id of the import in the import table.
            import_command (str): The command indicating the step to be performed in the import process.
                                  It can be either "validate" or "process".
        Returns:
            str: The result of the step performed in the import process, or "invalid command"
        """
        step = self.command_map.get(import_command)
        if not step:
            return 'invalid command'
        return self.do_import_step(import_id, step)

    def do_import_step(self, import_id: str, step: str) -> str:
        """
        Perform the given step in the import process.
        Args:
            import_id (str): The id of the import in the import table.
            step (str): The step indicating whether the row should be validated or processed.
        Returns:
            str: The result of the step performed in the import process, or "import cancelled" if a user cancelled the import
        """
        rows_completed = 0
        self.Import.set_import_step_start_time(import_id, step)
        username = self.Import.get_import_step_initiated_by_user(import_id, step)
        import_row_count = self.ImportRow.get_import_step_incomplete_row_count(import_id, step)
        if import_row_count == 0:
            return f'no rows need {step} for import_id {import_id}'
        while rows_completed < import_row_count:
            logging.info(f'getting next batch for import_id: {import_id}, step: {step}, rows_completed: {rows_completed}')
            import_row_batch = self.ImportRow.get_import_step_rows(import_id, step, IMPORT_BATCH_SIZE)      
            completed_row_ids = []
            for row in import_row_batch:
                self.validate_or_process_row(row, step, username)
                rows_completed += 1
                completed_row_ids.append(row.row_id)
            self.ImportRow.set_import_row_step_complete(completed_row_ids, step)
            if not self.should_continue_import(import_id, rows_completed, step):
                return 'import cancelled'
        return 'import complete'

    def validate_or_process_row(self, row: Any, step: str, username: str) -> bool:
        """
        Sends a POST request to either the validation or processing URL based on the given step parameter.
        Checks the response received from the request and handles any validation errors by inserting them into the database.
        Args:
            row (Any): The row object to be validated or processed. sqlalchemy query result object
            step (str): The step indicating whether the row should be validated or processed.
        Returns:
            bool: True if the row was successfully validated or processed, False otherwise.
        """
        logging.info(f'sent data {row.data}')
        url = self.validate_url if step == 'validation' else self.process_url
        response = self.session.post(f'{url}/{row.worksheet}/{username}', data=row.data)
        if response.status_code != 200:
            logging.error(f'Received unexpected return code from curl request {response.status_code}')
            return False
        try:
            response_data = json.loads(response.text)
        except Exception as e:
            logging.exception(f'Invalid response received from {step} url, exception: {str(e)}')
            response_data = {'valid': False, 'error_message': f'Invalid response received from {step} url'}
        if step == 'validation' and not response_data.get('valid', False):
            error_message = response_data.get('error_message', 'Validation failed with no error message')
            if isinstance(error_message, list):
                error_message = str(error_message[0]) if len(error_message) == 1 else ', '.join(error_message)
            self.ImportValidationError.insert_validation_error(row.import_id, row.row_id, error_message)
        return True

    def should_continue_import(self, import_id: str, rows_completed: int, step: str) -> bool:
        """
        Determines whether the import process should continue based on cancellation, or validation errors exceeding a threshold.
        Args:
            import_id (str): The id of the import in the import table.
            rows_completed (int): The number of rows that have been completed in the current step.
        Returns:
            bool: True if the import process should continue, False if it should be cancelled.
        """
        cancelled_by_user = self.Import.is_import_cancelled(import_id)
        if cancelled_by_user:
            logging.info(f'validate import cancelled by user: {cancelled_by_user}, import_id: {import_id}')
            return False
        if step == 'validation' and rows_completed >= 100:
            error_count = self.ImportValidationError.get_validation_errors(import_id)
            if error_count >= 100:
                logging.info(f'validate import cancelled due to too many validation errors import_id: {import_id}')
                return False
        return True
