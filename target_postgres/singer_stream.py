from copy import deepcopy
import json
import uuid

import arrow
from jsonschema import Draft4Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from target_postgres import json_schema, singer
from target_postgres.exceptions import SingerStreamError


SINGER_RECEIVED_AT = '_sdc_received_at'
SINGER_BATCHED_AT = '_sdc_batched_at'
SINGER_SEQUENCE = '_sdc_sequence'
SINGER_TABLE_VERSION = '_sdc_table_version'
SINGER_PK = '_sdc_primary_key'
SINGER_SOURCE_PK_PREFIX = '_sdc_source_key_'
SINGER_LEVEL = '_sdc_level_{}_id'
SINGER_VALUE = '_sdc_value'

RAW_LINE_SIZE = '__raw_line_size'


def get_line_size(line_data):
    return line_data.get(RAW_LINE_SIZE) or len(json.dumps(line_data))


class BufferedSingerStream():
    def __init__(self,
                 stream,
                 schema,
                 key_properties,
                 *args,
                 invalid_records_detect=None,
                 invalid_records_threshold=None,
                 max_rows=200000,
                 max_buffer_size=104857600,  # 100MB
                 **kwargs):
        """
        :param invalid_records_detect: Defaults to True when value is None
        :param invalid_records_threshold: Defaults to 0 when value is None
        """
        self.schema = None
        self.key_properties = None
        self.validator = None
        self.update_schema(schema, key_properties)

        self.stream = stream
        self.invalid_records = []
        self.max_rows = max_rows
        self.max_buffer_size = max_buffer_size

        self.invalid_records_detect = invalid_records_detect
        self.invalid_records_threshold = invalid_records_threshold

        if self.invalid_records_detect is None:
            self.invalid_records_detect = True
        if self.invalid_records_threshold is None:
            self.invalid_records_threshold = 0

        self.__buffer = []
        self.__count = 0
        self.__size = 0
        self.__lifetime_max_version = None

    def update_schema(self, schema, key_properties):
        # In order to determine whether a value _is in_ properties _or not_ we need to flatten `$ref`s etc.
        self.schema = json_schema.simplify(schema)
        self.key_properties = deepcopy(key_properties)

        # The validator can handle _many_ more things than our simplified schema, and is, in general handled by third party code
        self.validator = Draft4Validator(schema, format_checker=FormatChecker())

        try:
            properties = self.schema['properties']
        except KeyError:
            self.schema = self.schema['anyOf'][0]
            properties = self.schema['properties']

        if singer.RECEIVED_AT not in properties:
            properties[singer.RECEIVED_AT] = {
                'type': ['null', 'string'],
                'format': 'date-time'
            }

        if singer.SEQUENCE not in properties:
            properties[singer.SEQUENCE] = {
                'type': ['null', 'integer']
            }

        if singer.TABLE_VERSION not in properties:
            properties[singer.TABLE_VERSION] = {
                'type': ['null', 'integer']
            }

        if singer.BATCHED_AT not in properties:
            properties[singer.BATCHED_AT] = {
                'type': ['null', 'string'],
                'format': 'date-time'
            }

        if len(self.key_properties) == 0:
            self.use_uuid_pk = True
            self.key_properties = [singer.PK]
            properties[singer.PK] = {
                'type': ['string']
            }
        else:
            self.use_uuid_pk = False

    @property
    def count(self):
        return self.__count

    @property
    def buffer_full(self):
        if self.__count >= self.max_rows:
            return True

        if self.__count > 0:
            if self.__size >= self.max_buffer_size:
                return True

        return False

    @property
    def max_version(self):
        return self.__lifetime_max_version

    def __update_version(self, version):
        if version is None or (self.__lifetime_max_version is not None and self.__lifetime_max_version >= version):
            return None

        ## TODO: log warning about earlier records detected

        self.flush_buffer()
        self.__lifetime_max_version = version

    def add_record_message(self, record_message):
        add_record = True

        self.__update_version(record_message.get('version'))

        if self.__lifetime_max_version != record_message.get('version'):
            return None

        try:
            self.validator.validate(record_message['record'])
        except ValidationError as error:
            add_record = False
            self.invalid_records.append((error, record_message))

        if add_record:
            self.__buffer.append(record_message)
            self.__size += get_line_size(record_message)
            self.__count += 1
        elif self.invalid_records_detect \
                and len(self.invalid_records) >= self.invalid_records_threshold:
            raise SingerStreamError(
                'Invalid records detected above threshold: {}. See `.args` for details.'.format(
                    self.invalid_records_threshold),
                self.invalid_records)

    def peek_buffer(self):
        return self.__buffer

    def get_batch(self):
        current_time = arrow.get().format('YYYY-MM-DD HH:mm:ss.SSSSZZ')

        records = []
        for record_message in self.peek_buffer():
            record = record_message['record']

            if 'version' in record_message:
                record[singer.TABLE_VERSION] = record_message['version']

            if 'time_extracted' in record_message and record.get(singer.RECEIVED_AT) is None:
                record[singer.RECEIVED_AT] = record_message['time_extracted']

            if self.use_uuid_pk and record.get(singer.PK) is None:
                record[singer.PK] = str(uuid.uuid4())

            record[singer.BATCHED_AT] = current_time

            if 'sequence' in record_message:
                record[singer.SEQUENCE] = record_message['sequence']
            else:
                record[singer.SEQUENCE] = arrow.get().timestamp

            records.append(record)

        return records

    def flush_buffer(self):
        _buffer = self.__buffer
        self.__buffer = []
        self.__size = 0
        self.__count = 0
        return _buffer

    def peek_invalid_records(self):
        return self.invalid_records
