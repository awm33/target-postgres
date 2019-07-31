import http.client
import io
import pkg_resources
import sys
import threading

from singer import utils, metadata, metrics

from target_postgres.exceptions import TargetError
from target_postgres.globals import LOGGER
from target_postgres.pipes.load import load
from target_postgres.singer_stream import BufferedSingerStream
from target_postgres.stream_tracker import StreamTracker


def main(target):
    """
    Given a target, stream stdin input as a text stream.
    :param target: object which implements `write_batch` and `activate_version`
    :return: None
    """
    config = utils.parse_args([]).config
    input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    stream_to_target(input_stream, target, config=config)

    return None


def stream_to_target(stream, target, config={}):
    """
    Persist `stream` to `target` with optional `config`.
    :param stream: iterator which represents a Singer data stream
    :param target: object which implements `write_batch` and `activate_version`
    :param config: [optional] configuration for buffers etc.
    :return: None
    """

    state_support = config.get('state_support', True)
    state_tracker = StreamTracker(target, state_support)

    try:
        if not config.get('disable_collection', False):
            _async_send_usage_stats()

        invalid_records_detect = config.get('invalid_records_detect')
        invalid_records_threshold = config.get('invalid_records_threshold')
        max_batch_rows = config.get('max_batch_rows')
        max_batch_size = config.get('max_batch_size')
        batch_detection_threshold = config.get('batch_detection_threshold', 5000)

        line_count = 0
        for line_data in load(stream):
            _line_handler(state_tracker,
                          target,
                          invalid_records_detect,
                          invalid_records_threshold,
                          max_batch_rows,
                          max_batch_size,
                          line_data)
            if line_count > 0 and line_count % batch_detection_threshold == 0:
                state_tracker.flush_streams()
            line_count += 1

        state_tracker.flush_streams(force=True)

        return None

    except Exception as e:
        LOGGER.critical(e)
        raise e
    finally:
        _report_invalid_records(state_tracker.streams)


def _report_invalid_records(streams):
    for stream_buffer in streams.values():
        if stream_buffer.peek_invalid_records():
            LOGGER.warning("Invalid records detected for stream {}: {}".format(
                stream_buffer.stream,
                stream_buffer.peek_invalid_records()
            ))


def _line_handler(state_tracker,
                  target,
                  invalid_records_detect, invalid_records_threshold, max_batch_rows, max_batch_size,
                  line_data):
    if line_data['type'] == 'SCHEMA':
        stream = line_data['stream']

        schema = line_data['schema']

        key_properties = line_data.get('key_properties', None)

        if stream not in state_tracker.streams:
            buffered_stream = BufferedSingerStream(stream,
                                                   schema,
                                                   key_properties,
                                                   invalid_records_detect=invalid_records_detect,
                                                   invalid_records_threshold=invalid_records_threshold)
            if max_batch_rows:
                buffered_stream.max_rows = max_batch_rows
            if max_batch_size:
                buffered_stream.max_buffer_size = max_batch_size

            state_tracker.register_stream(stream, buffered_stream)
        else:
            state_tracker.streams[stream].update_schema(schema, key_properties)

    elif line_data['type'] == 'RECORD':
        state_tracker.handle_record_message(line_data['stream'], line_data)

    elif line_data['type'] == 'ACTIVATE_VERSION':
        if line_data['stream'] not in state_tracker.streams:
            raise TargetError('A ACTIVATE_VERSION for stream {} was encountered before a corresponding schema'
                              .format(line_data['stream']))

        stream_buffer = state_tracker.streams[line_data['stream']]
        target.write_batch(stream_buffer)
        target.activate_version(stream_buffer, line_data['version'])

    elif line_data['type'] == 'STATE':
        state_tracker.handle_state_message(line_data)
    else:
        raise TargetError('Unknown message type {} in message {}'.format(
            line_data['type'],
            line_data))


def _send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-postgres').version
        with http.client.HTTPConnection('collector.singer.io', timeout=10).connect() as conn:
            params = {
                'e': 'se',
                'aid': 'singer',
                'se_ca': 'target-postgres',
                'se_ac': 'open',
                'se_la': version,
            }
            conn.request('GET', '/i?' + urllib.parse.urlencode(params))
            conn.getresponse()
    except:
        LOGGER.debug('Collection request failed')


def _async_send_usage_stats():
    LOGGER.info('Sending version information to singer.io. ' +
                'To disable sending anonymous usage data, set ' +
                'the config parameter "disable_collection" to true')
    threading.Thread(target=_send_usage_stats()).start()
