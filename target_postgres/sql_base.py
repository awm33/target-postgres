# SQL Base
## This module is the base implementation for Singer SQL target support.
## Expected usage of this module is to create a class representing your given
## SQL Target which overrides SQLInterface.
#
# Transition
## The given implementation here is in transition as we expand and add various
## targets. As such, there are many private helper functions which are providing
## the real support.
##
## The expectation is that these functions will be added to SQLInterface as we
## better understand how to make adding new targets simpler.
#

from copy import deepcopy
from functools import lru_cache, partial
import pickle
import time

import singer
import singer.metrics as metrics

from target_postgres import denest
from target_postgres import json_schema

SEPARATOR = '__'
CURRENT_SCHEMA_VERSION = 2


def _duration_millis(start):
    return int((time.monotonic() - start) * 1000)


def _mapping_name(field, schema):
    return field + SEPARATOR + json_schema.shorthand(schema)


class SQLInterface:
    """
    Generic interface for handling SQL Targets in Singer.

    Provides reasonable defaults for:
    - nested schemas -> traditional SQL Tables and Columns
    - nested records -> traditional SQL Table rows

    Expected usage for use with your given target is to:
    - override all public _non-helper_ functions
    - use all public _helper_ functions inside of your _non-helper_ functions

    Function Syntax:
    - `_...` prefix : Private function
    - `..._helper` suffix : Helper function
    """

    IDENTIFIER_FIELD_LENGTH = NotImplementedError('`IDENTIFIER_FIELD_LENGTH` not implemented.')
    LOGGER = singer.get_logger()

    def _set_timer_tags(self, metric, job_type, path):
        metric.tags['job_type'] = job_type
        metric.tags['path'] = path

        metric.tags.update(self.metrics_tags())

        return metric

    def _set_counter_tags(self, metric, counter_type, path):
        metric.tags['count_type'] = counter_type
        metric.tags['path'] = path

        metric.tags.update(self.metrics_tags())

        return metric

    def _set_metrics_tags__table(self, metric, table_name):
        metric.tags['table'] = table_name

        return metric

    def metrics_tags(self):
        """
        Optional function to overwrite to include more tags into Singer Metrics.
        :return: Dictonary of Tags
        """
        return {}

    def json_schema_to_sql_type(self, schema):
        """
        Given a JSONSchema structure, return a compatible string representing a SQL column type.
        :param schema: JSONSchema
        :return: string
        """
        raise NotImplementedError('`` not implemented.')

    def get_table_schema(self, connection, name):
        """
        Fetch the `table_schema` for `name`.

        :param connection: remote connection, type left to be determined by implementing class
        :param name: string
        :return: TABLE_SCHEMA(remote)
        """
        raise NotImplementedError('`get_table_schema` not implemented.')

    def _get_table_schema(self, connection, name):
        """
        get_table_schema, but with checking the version of the schema to ensure latest format.

        :param connection: remote connection, type left to be determined by implementing class
        :param name: string
        :return: TABLE_SCHEMA(remote)
        """
        remote_schema = self.get_table_schema(connection, name)
        if remote_schema and remote_schema.get('schema_version', 0) != CURRENT_SCHEMA_VERSION:
            raise Exception('Schema for `{}` is of version {}. Expected version {}'.format(
                name,
                remote_schema.get('schema_version', 0),
                CURRENT_SCHEMA_VERSION
            ))

        return remote_schema

    def is_table_empty(self, connection, name):
        """
        Returns True when given table name has no rows.

        :param connection: remote connection, type left to be determined by implementing class
        :param name: string
        :return: boolean
        """
        raise NotImplementedError('`is_table_empty` not implemented.')

    def canonicalize_identifier(self, name):
        """
        Given a SQL Identifier `name`, attempt to serialize it to an acceptable name for remote.
        NOTE: DOES NOT handle collision support, nor identifier length/truncation support.

        :param name: string
        :return: string
        """
        raise NotImplementedError('`canonicalize_identifier` not implemented.')

    def fetch_column_from_path(self, path, table_schema):
        """
        Should only be used for paths which have been established, ie, the schema will
        not be changing etc.
        :param path:
        :param table_schema:
        :return:
        """

        for to, m in table_schema.get('mappings', {}).items():
            if tuple(m['from']) == path:
                return to, json_schema.simple_type(m)

        raise Exception('blahbittyblah')

    def _canonicalize_column_identifier(self, path, schema, mappings):
        """"""

        from_type__to_name = {}
        existing_paths = set()
        existing_column_names = set()

        for m in mappings:
            from_type__to_name[(m['from'], json_schema.shorthand(m))] = m['to']
            existing_paths.add(m['from'])
            existing_column_names.add(m['to'])

        ## MAPPING EXISTS, NO CANONICALIZATION NECESSARY
        if (path, json_schema.shorthand(schema)) in from_type__to_name:
            return from_type__to_name[(path, json_schema.shorthand(schema))]

        raw_canonicalized_column_name = self.canonicalize_identifier(SEPARATOR.join(path))
        canonicalized_column_name = self.canonicalize_identifier(raw_canonicalized_column_name[:self.IDENTIFIER_FIELD_LENGTH])

        raw_suffix = ''
        ## NO TYPE MATCH
        if path in existing_paths:
            raw_suffix = SEPARATOR + json_schema.shorthand(schema)
            canonicalized_column_name = self.canonicalize_identifier(
                                          raw_canonicalized_column_name[
                                            :self.IDENTIFIER_FIELD_LENGTH - len(raw_suffix)] + raw_suffix)

            self.LOGGER.warning(
                'FIELD COLLISION: Field `{}` exists in remote already. No compatible type found. Appending type suffix: `{}`'.format(
                    path,
                    canonicalized_column_name
                ))

        i = 0
        ## NAME COLLISION
        while canonicalized_column_name in existing_column_names:
            self.LOGGER.warning(
                'NAME COLLISION: Field `{}` collided with `{}` in remote. Adding new integer suffix...'.format(
                    path,
                    canonicalized_column_name
                ))

            i += 1
            suffix = raw_suffix + SEPARATOR + str(i)
            canonicalized_column_name = self.canonicalize_identifier(
                                          raw_canonicalized_column_name[
                                            :self.IDENTIFIER_FIELD_LENGTH - len(suffix)] + suffix)

        return canonicalized_column_name

    def add_table(self, connection, path, name, metadata):
        """
        Create the remote table schema.

        :param connection: remote connection, type left to be determined by implementing class
        :param path: (String, ...)
        :param name: String
        :param metadata: additional metadata needed by implementing class
        :return: None
        """
        raise NotImplementedError('`add_table` not implemented.')

    def add_key_properties(self, connection, table_name, key_properties):
        """

        :param connection: remote connection, type left to be determined by implementing class
        :param table_name: string
        :param key_properties: [string, ...]
        :return: None
        """
        raise NotImplementedError('`add_key_properties` not implemented.')

    def add_table_mapping_helper(self, from_path, table_mappings):
        """

        :param from_path:
        :param table_mappings:
        :return: (boolean, string)
        """

        ## MAPPING EXISTS
        if from_path in table_mappings:
            return {'exists': True, 'to': table_mappings[from_path]}

        to_from = dict([(v, k) for k, v in table_mappings.items()])

        name = SEPARATOR.join(from_path)

        raw_canonicalized_name = self.canonicalize_identifier(name)
        canonicalized_name = self.canonicalize_identifier(raw_canonicalized_name[:self.IDENTIFIER_FIELD_LENGTH])

        i = 0
        ## NAME COLLISION
        while canonicalized_name in to_from:
            self.LOGGER.warning(
                'NAME COLLISION: Table `{}` collided with `{}` in remote. Adding new integer suffix...'.format(
                    from_path,
                    canonicalized_name
                ))

            i += 1
            suffix = SEPARATOR + str(i)
            canonicalized_name = self.canonicalize_identifier(raw_canonicalized_name[
                                   :self.IDENTIFIER_FIELD_LENGTH - len(suffix)] + suffix)

        return {'exists': False, 'to': canonicalized_name}

    def add_table_mapping(self, connection, from_path, metadata):
        """
        Given a full path to a table, `from_path`, add a table mapping to the canonicalized name.

        :param connection: remote connection, type left to be determined by implementing class
        :param from_path: (string, ...)
        :return: None
        """
        raise NotImplementedError('`add_table_mapping` not implemented.')

    def add_column(self, connection, table_name, name, schema):
        """
        Add column `name` in `table_name` with `schema`.

        :param connection: remote connection, type left to be determined by implementing class
        :param table_name: string
        :param name: string
        :param schema: JSON Object Schema
        :return: None
        """
        raise NotImplementedError('`add_column` not implemented.')

    def drop_column(self, connection, table_name, name):
        """
        Drop column `name` in `table_name`.

        :param connection: remote connection, type left to be determined by implementing class
        :param table_name: string
        :param name: string
        :return: None
        """
        raise NotImplementedError('`add_column` not implemented.')

    def migrate_column(self, connection, table_name, from_column, to_column):
        """
        Migrate data `from_column` in `table_name` `to_column`.

        :param connection: remote connection, type left to be determined by implementing class
        :param table_name: string
        :param from_column: string
        :param to_column: string
        :return: None
        """
        raise NotImplementedError('`migrate_column` not implemented.')

    def make_column_nullable(self, connection, table_name, name):
        """
        Update column `name` in `table_name` to accept `null` values.

        :param connection: remote connection, type left to be determined by implementing class
        :param table_name: string
        :param name: string
        :return: None
        """
        raise NotImplementedError('`make_column_nullable` not implemented.')

    def add_index(self, connection, table_name, column_names):
        """
        Add an index on a group of `column_names` in `table_name`.

        :param connection: remote connection, type left to be determined by implementing class
        :param table_name: string
        :param column_names: (string, ...)
        :return: None
        """
        raise NotImplementedError('`add_index` not implemented.')

    def add_column_mapping(self, connection, table_name, from_path, to_name, schema):
        """
        Given column path `from_path` add a column mapping to `to_name` for `schema`. A column mapping is an entry
        in the TABLE_SCHEMA which reads:

        {...
         'mappings': [...
           `to_name`: {'type': `json_schema.get_type(schema)`,
                       'from': `path`}
         ]
         ...}

        :param connection: remote connection, type left to be determined by implementing class
        :param table_name: string
        :param from_path: (string, ...)
        :param to_name: string
        :param schema: JSON Object Schema
        :return: None
        """
        raise NotImplementedError('`add_column_mapping` not implemented.')

    def drop_column_mapping(self, connection, table_name, name):
        """
        Given column mapping `name`, remove from the TABLE_SCHEMA(remote).

        :param connection: remote connection, type left to be determined by implementing class
        :param table_name: string
        :param name: string
        :return: None
        """
        raise NotImplementedError('`remove_column_mapping` not implemented.')

    def _get_mapping(self, existing_schema, path, schema):
        for to, mapping in existing_schema.get('mappings', {}).items():
            if tuple(mapping['from']) == path \
                    and json_schema.shorthand(mapping) == json_schema.shorthand(schema):
                return to

        return None

    def upsert_table_helper(self, connection, schema, metadata, log_schema_changes=True):
        """
        Upserts the `schema` to remote by:
        - creating table if necessary
        - adding columns
        - adding column mappings
        - migrating data from old columns to new, etc.

        :param connection: remote connection, type left to be determined by implementing class
        :param schema: TABLE_SCHEMA(local)
        :param metadata: additional information necessary for downstream operations,
        :param log_schema_changes: defaults to True, set to false to disable logging of table level schema changes
        :return: TABLE_SCHEMA(remote)
        """
        table_path = schema['path']

        with self._set_timer_tags(metrics.job_timer(),
                                  'upsert_table_schema',
                                  table_path) as timer:

            _metadata = deepcopy(metadata)
            _metadata['schema_version'] = CURRENT_SCHEMA_VERSION

            table_name = self.add_table_mapping(connection, table_path, _metadata)

            self._set_metrics_tags__table(timer, table_name)

            existing_schema = self._get_table_schema(connection, table_name)

            existing_table = True
            if existing_schema is None:
                self.add_table(connection, table_path, table_name, _metadata)
                existing_schema = self._get_table_schema(connection, table_name)
                existing_table = False

            self.add_key_properties(connection, table_name, schema.get('key_properties', None))

            ## Build up mappings to compare new columns against existing
            mappings = []

            for to, m in existing_schema.get('mappings', {}).items():
                mapping = json_schema.simple_type(m)
                mapping['from'] = tuple(m['from'])
                mapping['to'] = to
                mappings.append(mapping)

            ## Only process columns which have single, nullable, types
            column_paths_seen = set()
            single_type_columns = []

            for column_path, column_schema in schema['schema']['properties'].items():
                column_paths_seen.add(column_path)
                for sub_schema in column_schema['anyOf']:
                    single_type_columns.append((column_path, deepcopy(sub_schema)))

            ### Add any columns missing from new schema
            for m in mappings:
                if not m['from'] in column_paths_seen:
                    single_type_columns.append((m['from'], json_schema.make_nullable(m)))

            ## Process new columns against existing
            table_empty = self.is_table_empty(connection, table_name)

            for column_path, column_schema in single_type_columns:
                upsert_table_helper__start__column = time.monotonic()

                canonicalized_column_name = self._canonicalize_column_identifier(column_path, column_schema, mappings)
                nullable_column_schema = json_schema.make_nullable(column_schema)

                def log_message(msg):
                    if log_schema_changes:
                        self.LOGGER.info(
                            'Table Schema Change [`{}`.`{}`:`{}`] {} (took {} millis)'.format(
                                table_name,
                                column_path,
                                canonicalized_column_name,
                                msg,
                                _duration_millis(upsert_table_helper__start__column)))

                ## NEW COLUMN
                if not column_path in [m['from'] for m in mappings]:
                    upsert_table_helper__column = "New column"
                    ### NON EMPTY TABLE
                    if not table_empty:
                        upsert_table_helper__column += ", non empty table"
                        self.LOGGER.warning(
                            'NOT EMPTY: Forcing new column `{}` in table `{}` to be nullable due to table not empty.'.format(
                                column_path,
                                table_name))
                        column_schema = nullable_column_schema

                    self.add_column(connection,
                                    table_name,
                                    canonicalized_column_name,
                                    column_schema)
                    self.add_column_mapping(connection,
                                            table_name,
                                            column_path,
                                            canonicalized_column_name,
                                            column_schema)

                    mapping = json_schema.simple_type(column_schema)
                    mapping['from'] = column_path
                    mapping['to'] = canonicalized_column_name
                    mappings.append(mapping)

                    log_message(upsert_table_helper__column)

                    continue

                ## EXISTING COLUMNS
                ### SCHEMAS MATCH
                if [True for m in mappings if
                    m['from'] == column_path
                    and self.json_schema_to_sql_type(m) == self.json_schema_to_sql_type(column_schema)]:
                    continue
                ### NULLABLE SCHEMAS MATCH
                ###  New column _is not_ nullable, existing column _is_
                if [True for m in mappings if
                    m['from'] == column_path
                    and self.json_schema_to_sql_type(m) == self.json_schema_to_sql_type(nullable_column_schema)]:
                    continue

                ### NULL COMPATIBILITY
                ###  New column _is_ nullable, existing column is _not_
                non_null_original_column = [m for m in mappings if
                                            m['from'] == column_path and json_schema.shorthand(
                                                m) == json_schema.shorthand(column_schema)]
                if non_null_original_column:
                    ## MAKE NULLABLE
                    self.make_column_nullable(connection,
                                              table_name,
                                              canonicalized_column_name)
                    self.drop_column_mapping(connection, table_name, canonicalized_column_name)
                    self.add_column_mapping(connection,
                                            table_name,
                                            column_path,
                                            canonicalized_column_name,
                                            nullable_column_schema)

                    mappings = [m for m in mappings if not (m['from'] == column_path and json_schema.shorthand(
                        m) == json_schema.shorthand(column_schema))]

                    mapping = json_schema.simple_type(nullable_column_schema)
                    mapping['from'] = column_path
                    mapping['to'] = canonicalized_column_name
                    mappings.append(mapping)

                    log_message("Made existing column nullable.")

                    continue

                ### FIRST MULTI TYPE
                ###  New column matches existing column path, but the types are incompatible
                duplicate_paths = [m for m in mappings if m['from'] == column_path]

                if 1 == len(duplicate_paths):
                    existing_mapping = duplicate_paths[0]
                    existing_column_name = existing_mapping['to']

                    if existing_column_name:
                        self.drop_column_mapping(connection, table_name, existing_column_name)

                    ## Update existing properties
                    mappings = [m for m in mappings if m['from'] != column_path]

                    mapping = json_schema.simple_type(nullable_column_schema)
                    mapping['from'] = column_path
                    mapping['to'] = canonicalized_column_name
                    mappings.append(mapping)

                    existing_column_new_normalized_name = self._canonicalize_column_identifier(column_path,
                                                                                               existing_mapping,
                                                                                               mappings)

                    mapping = json_schema.simple_type(json_schema.make_nullable(existing_mapping))
                    mapping['from'] = column_path
                    mapping['to'] = existing_column_new_normalized_name
                    mappings.append(mapping)

                    ## Add new columns
                    ### NOTE: all migrated columns will be nullable and remain that way

                    #### Table Metadata
                    self.add_column_mapping(connection,
                                            table_name,
                                            column_path,
                                            existing_column_new_normalized_name,
                                            json_schema.make_nullable(existing_mapping))
                    self.add_column_mapping(connection,
                                            table_name,
                                            column_path,
                                            canonicalized_column_name,
                                            nullable_column_schema)

                    #### Columns
                    self.add_column(connection,
                                    table_name,
                                    existing_column_new_normalized_name,
                                    json_schema.make_nullable(existing_mapping))

                    self.add_column(connection,
                                    table_name,
                                    canonicalized_column_name,
                                    nullable_column_schema)

                    ## Migrate existing data
                    self.migrate_column(connection,
                                        table_name,
                                        existing_mapping['to'],
                                        existing_column_new_normalized_name)

                    ## Drop existing column
                    self.drop_column(connection,
                                     table_name,
                                     existing_mapping['to'])

                    upsert_table_helper__column = "Splitting `{}` into `{}` and `{}`. New column matches existing column path, but the types are incompatible.".format(
                        existing_column_name,
                        existing_column_new_normalized_name,
                        canonicalized_column_name
                    )

                ## REST MULTI TYPE
                elif 1 < len(duplicate_paths):
                    ## Add new column
                    self.add_column_mapping(connection,
                                            table_name,
                                            column_path,
                                            canonicalized_column_name,
                                            nullable_column_schema)
                    self.add_column(connection,
                                    table_name,
                                    canonicalized_column_name,
                                    nullable_column_schema)

                    mapping = json_schema.simple_type(nullable_column_schema)
                    mapping['from'] = column_path
                    mapping['to'] = canonicalized_column_name
                    mappings.append(mapping)

                    upsert_table_helper__column = "Adding new column to split column `{}`. New column matches existing column's path, but no types were compatible.".format(
                        column_path
                    )

                ## UNKNOWN
                else:
                    raise Exception(
                        'UNKNOWN: Cannot handle merging column `{}` (canonicalized as: `{}`) in table `{}`.'.format(
                            column_path,
                            canonicalized_column_name,
                            table_name
                        ))

                log_message(upsert_table_helper__column)

            if not existing_table:
                for column_names in self.new_table_indexes(schema):
                    self.add_index(connection, table_name, column_names)

            return self._get_table_schema(connection, table_name)

    def _serialize_table_record_field_name(self, remote_schema, path, value_json_schema_tuple):
        """
        Returns the appropriate remote field (column) name for `path`.

        :param remote_schema: TABLE_SCHEMA(remote)
        :param path: (string, ...)
        :value_json_schema: tuple, JSON Schema
        :return: string
        """

        # rebuild the dict that needs to be passed further down the call stack
        if len(value_json_schema_tuple) == 1:
            value_json_schema = { 'type': value_json_schema_tuple[0] }
        else:
            value_json_schema = {'type': value_json_schema_tuple[0],
                     'format': value_json_schema_tuple[1]}

        simple_json_schema = json_schema.simple_type(value_json_schema)

        mapping = self._get_mapping(remote_schema,
                                    path,
                                    simple_json_schema)

        if not mapping is None:
            return mapping

        ## Numbers are valid as `float` OR `int`
        ##  ie, 123.0 and 456 are valid 'number's
        if json_schema.INTEGER in json_schema.get_type(simple_json_schema):
            mapping = self._get_mapping(remote_schema,
                                        path,
                                        {'type': json_schema.NUMBER})

            if not mapping is None:
                return mapping

        raise Exception("A compatible column for path {} and JSONSchema {} in table {} cannot be found.".format(
            path,
            simple_json_schema,
            remote_schema['path']
        ))

    def serialize_table_record_null_value(
            self, remote_schema, streamed_schema, field, value):
        """
        Returns the serialized version of `value` which is appropriate for the target's null
        implementation.

        :param remote_schema: TABLE_SCHEMA(remote)
        :param streamed_schema: TABLE_SCHEMA(local)
        :param field: string
        :param value: literal
        :return: literal
        """
        raise NotImplementedError('`parse_table_record_serialize_null_value` not implemented.')

    def serialize_table_record_datetime_value(
            self, remote_schema, streamed_schema, field, value):
        """
        Returns the serialized version of `value` which is appropriate  for the target's datetime
        implementation.

        :param remote_schema: TABLE_SCHEMA(remote)
        :param streamed_schema: TABLE_SCHEMA(local)
        :param field: string
        :param value: literal
        :return: literal
        """

        raise NotImplementedError('`parse_table_record_serialize_datetime_value` not implemented.')

    def _serialize_table_records(
            self, remote_schema, streamed_schema, records):
        """
        Parse the given table's `records` in preparation for persistence to the remote target.

        Base implementation returns a list of dictionaries, where _every_ dictionary has the
        same keys as `remote_schema`'s properties.

        :param remote_schema: TABLE_SCHEMA(remote)
        :param streamed_schema: TABLE_SCHEMA(local)
        :param records: [{(path_0, path_1, ...): (_json_schema_string_type, value), ...}, ...]
        :return: [{...}, ...]
        """

        datetime_paths = set()
        default_paths = {}

        for column_path, column_schema in streamed_schema['schema']['properties'].items():
            for sub_schema in column_schema['anyOf']:
                if json_schema.is_datetime(sub_schema):
                    datetime_paths.add(column_path)
                if sub_schema.get('default') is not None:
                    default_paths[column_path] = sub_schema.get('default')

        ## Get the default NULL value so we can assign row values when value is _not_ NULL
        NULL_DEFAULT = self.serialize_table_record_null_value(remote_schema, streamed_schema, None, None)

        serialized_rows = []

        remote_fields = set(remote_schema['schema']['properties'].keys())
        default_row = dict([(field, NULL_DEFAULT) for field in remote_fields])

        paths = streamed_schema['schema']['properties'].keys()

        ## create a partial function with only hashable args so we can use lru_cache on it
        _cached_field_name = partial(self._serialize_table_record_field_name, remote_schema)
        cached_field_name = lru_cache(maxsize=None)(_cached_field_name)

        for record in records:

            ## pickling/unpickling is much faster than deepcopy
            row = pickle.loads(pickle.dumps(default_row))

            for path in paths:
                json_schema_string_type, value = record.get(path, (None, None))

                ## Serialize fields which are not present but have default values set
                if path in default_paths \
                        and value is None:
                    value = default_paths[path]
                    json_schema_string_type = json_schema.python_type(value)

                if not json_schema_string_type:
                    continue

                ## Serialize datetime to compatible format
                if path in datetime_paths \
                        and json_schema_string_type == json_schema.STRING \
                        and value is not None:
                    value = self.serialize_table_record_datetime_value(remote_schema, streamed_schema, path,
                                                                       value)
                    value_json_schema_tuple = (json_schema.STRING, json_schema.DATE_TIME_FORMAT)
                    field_name = cached_field_name(path, value_json_schema_tuple)
                else:
                    field_name = cached_field_name(path, (json_schema_string_type,))

                ## Serialize NULL default value
                value = self.serialize_table_record_null_value(remote_schema, streamed_schema, path, value)

                ## `field_name` is unset
                if row[field_name] == NULL_DEFAULT:
                    row[field_name] = value

            serialized_rows.append(row)

        return serialized_rows

    def write_table_batch(self, connection, table_batch, metadata):
        """
        Update the remote for given table's schema, and write records. Returns the number of
        records persisted.

        :param connection: remote connection, type left to be determined by implementing class
        :param table_batch: {'remote_schema': TABLE_SCHEMA(remote),
                             'records': [{...}, ...]}
        :param metadata: additional metadata needed by implementing class
        :return: integer
        """
        raise NotImplementedError('`write_table_batch` not implemented.')

    def write_batch_helper(self, connection, root_table_name, schema, key_properties, records, metadata):
        """
        Write all `table_batch`s associated with the given `schema` and `records` to remote.

        :param connection: remote connection, type left to be determined by implementing class
        :param root_table_name: string
        :param schema: SingerStreamSchema
        :param key_properties: [string, ...]
        :param records: [{...}, ...]
        :param metadata: additional metadata needed by implementing class
        :return: {'records_persisted': int,
                  'rows_persisted': int}
        """
        with self._set_timer_tags(metrics.job_timer(),
                                  'batch',
                                  (root_table_name,)):
            with self._set_counter_tags(metrics.record_counter(None),
                                        'batch_rows_persisted',
                                        (root_table_name,)) as batch_counter:
                self.LOGGER.info('Writing batch with {} records for `{}` with `key_properties`: `{}`'.format(
                    len(records),
                    root_table_name,
                    key_properties
                ))

                for table_batch in denest.to_table_batches(schema, key_properties, records):
                    table_batch['streamed_schema']['path'] = (root_table_name,) + \
                                                             table_batch['streamed_schema']['path']

                    with self._set_timer_tags(metrics.job_timer(),
                                              'table',
                                              table_batch['streamed_schema']['path']) as table_batch_timer:
                        with self._set_counter_tags(metrics.record_counter(None),
                                                    'table_rows_persisted',
                                                    table_batch['streamed_schema']['path']) as table_batch_counter:
                            self.LOGGER.info('Writing table batch schema for `{}`...'.format(
                                table_batch['streamed_schema']['path']
                            ))

                            remote_schema = self.upsert_table_helper(connection,
                                                                     table_batch['streamed_schema'],
                                                                     metadata)

                            self._set_metrics_tags__table(table_batch_timer, remote_schema['name'])
                            self._set_metrics_tags__table(table_batch_counter, remote_schema['name'])

                            self.LOGGER.info('Writing table batch with {} rows for `{}`...'.format(
                                len(table_batch['records']),
                                table_batch['streamed_schema']['path']
                            ))

                            batch_rows_persisted = self.write_table_batch(
                                connection,
                                {'remote_schema': remote_schema,
                                 'records': self._serialize_table_records(remote_schema,
                                                                          table_batch['streamed_schema'],
                                                                          table_batch['records'])},
                                metadata)

                            table_batch_counter.increment(batch_rows_persisted)
                            batch_counter.increment(batch_rows_persisted)

                return {
                    'records_persisted': len(records),
                    'rows_persisted': batch_counter.value
                }

    def write_batch(self, stream_buffer):
        """
        Persist `stream_buffer.records` to remote.

        :param stream_buffer: SingerStreamBuffer
        :return: {'records_persisted': int,
                  'rows_persisted': int}
        """
        raise NotImplementedError('`write_batch` not implemented.')

    def activate_version(self, stream_buffer, version):
        """
        Activate the given `stream_buffer`'s remote to `version`

        :param stream_buffer: SingerStreamBuffer
        :param version: integer
        :return: boolean
        """
        raise NotImplementedError('`activate_version` not implemented.')

    def new_table_indexes(self, schema):
        """
        Returns a list of lists of string column names to add indexes for a new table once that new table has been fully created.
        For subclassess where indexes don't make any sense, like Redshift, this can safely always return false.

        :param schema: TABLE_SCHEMA(local)
        :param column_name: string
        :return: [[column_name: string], [column_name: string, column_name: string],...]
        """
        return []

