from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.sql import select, update, insert, func, asc
from sqlalchemy.schema import Table
from models.model_base import Base
from configs.db import ConnBase
import logging

apps_base = ConnBase('apps_db')
DB_QUERY_RETRY_LIMIT = 1

class Import(Base):
    __table__ = Table(
        'import',
        apps_base.metadata,
        autoload=True,
        autoload_with=apps_base.engine
    )

    def is_import_cancelled(self, import_id):
        query=(
            select([
                Import.cancelled_by_user
            ])
            .where(Import.import_id == import_id)
        )
        try:
            result = apps_base.session.execute(query).first()
            return result.cancelled_by_user if result.cancelled_by_user is not None else False
        except Exception as e:
            logging.exception(f'Import::is_import_cancelled query failed to process, {e}')
            apps_base.session.rollback()
            return False

    def set_import_step_start_time(self, import_id, step, retry_count=0):
        query = (
            update(Import)
                .where(Import.import_id == import_id)
        )
        if step == 'validation':
            query = query.values(validation_start_time = func.now())
        elif step == 'processing':
            query = query.values(processing_start_time = func.now())
        else:
            return False
        try:
            apps_base.session.execute(query)
            apps_base.session.commit()
        except Exception as e:
            logging.exception(f'Import::set_import_step_start_time query failed to process, {e}')
            apps_base.session.rollback()
            if retry_count < DB_QUERY_RETRY_LIMIT:
                logging.info(f'Import::set_import_step_start_time recursive retry #{retry_count+1}')
                return self.set_import_step_start_time(import_id, step, retry_count=retry_count+1)
            return False

    def get_import_step_initiated_by_user(self, import_id, step):
        if step == 'validation':
            query = (select([Import.uploaded_by_user.label('username')]))
        elif step == 'processing':
            query = (select([Import.processed_by_user.label('username')]))
        else:
            return False
        try:
            result = apps_base.session.execute(query.where(Import.import_id == import_id)).first()
            return result.username if result.username is not None else False
        except Exception as e:
            logging.exception(f'Import::get_import_step_initiated_by_user query failed to process, {e}')
            apps_base.session.rollback()
            return False

class ImportRow(Base):
    __table__ = Table(
        'import_row',
        apps_base.metadata,
        ForeignKeyConstraint(['import_id'], ['import_id']),
        autoload=True,
        autoload_with=apps_base.engine
    )

    def get_import_step_incomplete_row_count(self, import_id, step):
        query=(
            select([
                func.count().label('row_count')
            ])
            .where(ImportRow.import_id == import_id)
        )
        if step == 'validation':
            query = query.where(ImportRow.validated == 0)
        elif step == 'processing':
            query = query.where(ImportRow.processed == 0)
        else:
            return False
        try:
            result = apps_base.session.execute(query)
            row_count = result.scalar()
            return row_count
        except Exception as e:
            logging.exception(f'ImportRow::get_import_step_incomplete_row_count query failed to process, {e}')
            apps_base.session.rollback()
            return False


    def get_import_step_rows(self, import_id, step, batch_size = 51):
        query=(
            select([
                ImportRow.row_id,
                ImportRow.import_id,
                ImportRow.cs_no,
                ImportRow.worksheet,
                ImportRow.processed,
                ImportRow.validated,
                ImportRow.data
            ])
            .where(ImportRow.import_id == import_id)
        )
        if step == 'validation':
            query = query.where(ImportRow.validated == 0)
        elif step == 'processing':
            query = query.where(ImportRow.processed == 0)
        else:
            return False
        query = query.order_by(asc(ImportRow.row_id)).limit(batch_size)
        try:
            result = apps_base.session.execute(query).fetchall()
            return result
        except Exception as e:
            logging.exception(f'ImportRow::get_import_rows query failed to process, {e}')
            apps_base.session.rollback()
            return False


    def set_import_row_step_complete(self, row_ids, step):
        if row_ids is None or not row_ids:
            return
        query = (
            update(ImportRow)
                .where(ImportRow.row_id.in_(row_ids))
        )
        if step == 'validation':
            query = query.values(validated = 1)
        elif step == 'processing':
            query = query.values(processed = 1)
        else:
            return False
        try:
            apps_base.session.execute(query)
            apps_base.session.commit()
        except Exception as e:
            logging.exception(f'ImportRow::set_import_row_step_complete query failed to process, {e}')
            apps_base.session.rollback()
            return False

class ImportValidationError(Base):
    __table__ = Table(
        'import_validation_error',
        apps_base.metadata,
        ForeignKeyConstraint(['import_id'], ['import_id']),
        autoload=True,
        autoload_with=apps_base.engine
    )

    def get_validation_errors(self, import_id):
        query=(
            select([
                func.count().label('error_count')
            ])
            .where(ImportValidationError.import_id == import_id)
        )
        try:
            result = apps_base.session.execute(query)
            error_count = result.scalar()
            return error_count
        except Exception as e:
            logging.exception(f'ImportValidationError::get_validation_errors query failed to process, {e}')
            apps_base.session.rollback()
            return False

    def insert_validation_error(self,import_id,row_id,error_message):
        data = [{
            "import_id": import_id,
            "row_id": row_id,
            "error_message": error_message
        }]
        try:
            apps_base.session.execute(insert(ImportValidationError).values(data))
            apps_base.session.commit()
        except Exception as e:
            logging.exception(f'ImportValidationError::insert_validation_errors query failed to process {e}')
            apps_base.session.rollback()
            return False
