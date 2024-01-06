import pytest
import json
import requests
from unittest.mock import MagicMock, patch
from workers.import_worker import ImportWorker

def add_db_objects(import_worker):
    import_worker.Import = MagicMock()
    import_worker.ImportRow = MagicMock()
    import_worker.ImportValidationError = MagicMock()
    return import_worker

def mock_import_db_functions(mocker, import_worker, row):
    mocker.patch.object(import_worker.ImportRow, 'get_import_step_rows', return_value=[row, row, row])
    mocker.patch.object(import_worker.ImportRow, 'get_import_step_incomplete_row_count', return_value=3)
    mocker.patch.object(import_worker.ImportRow, 'set_import_row_step_complete', return_value=True)
    mocker.patch.object(import_worker.Import, 'set_import_step_start_time', return_value=[row, row, row])
    mocker.patch.object(import_worker.Import, 'get_import_step_initiated_by_user', return_value='username')
    mocker.patch.object(import_worker.ImportValidationError, 'get_validation_errors', return_value=0)
    mocker.patch.object(ImportWorker, 'validate_or_process_row')
    mocker.patch.object(ImportWorker, 'should_continue_import', return_value=True)
    return mocker

def create_import_worker():
    import_worker = ImportWorker()
    import_worker = add_db_objects(import_worker)
    return import_worker

def test_should_continue_import_cancelled_by_user(mocker):
    import_worker = create_import_worker()
    import_worker.Import.is_import_cancelled = mocker.Mock(return_value=True)
    assert False == import_worker.should_continue_import(import_id=1, rows_completed=1, step='validation')
    assert False == import_worker.should_continue_import(import_id=1000, rows_completed=1000, step='validation')

def test_should_continue_import(mocker):
    import_worker = create_import_worker()
    import_worker.Import.is_import_cancelled = mocker.Mock(return_value=False)
    import_worker.ImportValidationError.get_validation_errors = mocker.Mock(return_value=0)

    assert True == import_worker.should_continue_import(import_id=1, rows_completed=1, step='validation')
    assert True == import_worker.should_continue_import(import_id=1, rows_completed=100, step='validation')
    assert True == import_worker.should_continue_import(import_id=1, rows_completed=2000, step='validation')

    assert True == import_worker.should_continue_import(import_id=1, rows_completed=1, step='processing')
    assert True == import_worker.should_continue_import(import_id=1, rows_completed=100, step='processing')
    assert True == import_worker.should_continue_import(import_id=1, rows_completed=2000, step='processing')

    mocker.patch.object(import_worker.ImportValidationError, 'get_validation_errors', return_value=1000)
    assert True == import_worker.should_continue_import(import_id=1, rows_completed=1, step='validation')
    assert False == import_worker.should_continue_import(import_id=1, rows_completed=100, step='validation')
    assert False == import_worker.should_continue_import(import_id=1, rows_completed=2000, step='validation')

@patch.object(requests.Session,'post')
def test_validate_or_process_row(mock_post,mocker):
    row = MagicMock()
    row.import_id = 123
    row.row_id = 321
    import_worker = create_import_worker()
    import_worker.ImportValidationError.insert_validation_error = mocker.Mock(return_value=True)

    mock_post.return_value.status_code = 200
    mock_post.return_value.text = json.dumps({'valid': True})
    assert True == import_worker.validate_or_process_row(row,'validation','username')

    mock_post.return_value.status_code = 404
    assert False == import_worker.validate_or_process_row(row,'validation','username')

    mock_post.return_value.status_code = 200
    mock_post.return_value.text = json.dumps({'valid': False, 'error_message':['test error']})
    assert True == import_worker.validate_or_process_row(row,'validation','username')
    import_worker.ImportValidationError.insert_validation_error.assert_called_once_with(123, 321, 'test error')

def test_run_worker(mocker):
    mock = MagicMock()
    import_worker = create_import_worker()
    invalid_body = json.dumps({'asd':'123'}).encode('utf-8')
    assert False == import_worker.runWorker(mock,mock,mock,invalid_body)

    invalid_command = json.dumps({'import_id':'123','import_command':'testfail'}).encode('utf-8')
    assert False == import_worker.runWorker(mock,mock,mock,invalid_command)

    valid_body = json.dumps({'import_id':'123','import_command':'validate'}).encode('utf-8')
    mocker.patch.object(import_worker, 'handle_import_command', return_value=True)
    assert True == import_worker.runWorker(mock,mock,mock,valid_body)
    mocker.patch.object(import_worker, 'handle_import_command', side_effect=AttributeError)
    assert True == import_worker.runWorker(mock,mock,mock,valid_body)

def test_validates_all_rows(mocker):
    row = MagicMock()
    import_worker = create_import_worker()
    mocker = mock_import_db_functions(mocker, import_worker, row)

    result = import_worker.do_import_step(54321,'validation')

    assert result == 'import complete'
    assert ImportWorker.validate_or_process_row.call_count == 3
    assert ImportWorker.should_continue_import.called_once_with(54321, 3)

def test_processes_all_rows(mocker):
    row = MagicMock()
    import_worker = create_import_worker()
    mocker = mock_import_db_functions(mocker, import_worker, row)

    result = import_worker.do_import_step(54321,'process')

    # Assertions
    assert result == 'import complete'
    assert ImportWorker.validate_or_process_row.call_count == 3
    assert ImportWorker.should_continue_import.called_once_with(54321, 3)
