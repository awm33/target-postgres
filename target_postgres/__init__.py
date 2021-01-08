import os

from singer import utils
import psycopg2

from target_postgres.postgres import MillisLoggingConnection, PostgresTarget
from target_postgres import target_tools

REQUIRED_CONFIG_KEYS = [
    'postgres_database'
]


CONFIG_TO_ENV_MAPPING = {
    'postgres_host': 'PGHOST',
    'postgres_port': 'PGPORT',
    'postgres_database': 'PGDATABASE',
    'postgres_username': 'PGUSER',
    'postgres_password': 'PGPASSWORD',
    'postgres_sslmode': 'PGSSLMODE',
    'postgres_sslcert': 'PGSSLCERT',
    'postgres_sslkey': 'PGSSLKEY',
    'postgres_sslrootcert': 'PGSSLROOTCERT',
    'postgres_sslcrl': 'PGSSLCRL',
}


def main(config, input_stream=None):
    with psycopg2.connect(
            connection_factory=MillisLoggingConnection,
            host=config.get('postgres_host', 'localhost'),
            port=config.get('postgres_port', 5432),
            dbname=config.get('postgres_database'),
            user=config.get('postgres_username'),
            password=config.get('postgres_password'),
            sslmode=config.get('postgres_sslmode'),
            sslcert=config.get('postgres_sslcert'),
            sslkey=config.get('postgres_sslkey'),
            sslrootcert=config.get('postgres_sslrootcert'),
            sslcrl=config.get('postgres_sslcrl')
    ) as connection:
        postgres_target = PostgresTarget(
            connection,
            postgres_schema=config.get('postgres_schema', 'public'),
            logging_level=config.get('logging_level'),
            persist_empty_tables=config.get('persist_empty_tables'),
            add_upsert_indexes=config.get('add_upsert_indexes', True),
            before_run_sql=config.get('before_run_sql'),
            after_run_sql=config.get('after_run_sql'),
        )

        if input_stream:
            target_tools.stream_to_target(input_stream, postgres_target, config=config)
        else:
            target_tools.main(postgres_target)


def fallback_to_env_vars(config):
    for conf_key, env_var in CONFIG_TO_ENV_MAPPING.items():
        if config.get(conf_key) is None:
            config[conf_key] = os.environ.get(env_var)
    return config


def cli():
    args = utils.parse_args()
    config = fallback_to_env_vars(args.config)
    utils.check_config(config, REQUIRED_CONFIG_KEYS)

    main(config)
