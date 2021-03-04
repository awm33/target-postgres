# Contribution guidelines

## Setting up test environment

### Prerequisits

```
python3 -m virtualenv python3 venv
source venv/bin/activate
python3 -m pip install -e .[tests]
# python3 -m pip install -e .\[tests\] <- night need to escape on zsh
export POSTGRES_HOST=localhost
export POSTGRES_DATABASE=target_postgres_test
export POSTGRES_USERNAME=target_postgres_test
export POSTGRES_PASSWORD=target_postgres_test
```

#### Database setup
If you're not using the docker images for tests you'll need to set one up and
configure a user on it.

```
$ psql template1;
<>=# CREATE USER target_postgres_test WITH PASSWORD 'target_postgres_test';
<>=# CREATE DATABASE target_postgres_test WITH owner=target_postgres_test;
<>=# GRANT ALL privileges ON DATABASE target_postgres_test TO target_postgres_test;
```

#### If psycopg2 install fails

psycopg2 requires ssl and may fail the `pip install` process above

##### Installing openssl

###### OSX:

One possible solution is to use [homebrew](https://brew.sh/):

```
brew install openssl@1.1
export LDFLAGS="-L/usr/local/opt/openssl@1.1/lib"
export CPPFLAGS="-I/usr/local/opt/openssl@1.1/include
python3 -m pip install -r requirements_test.pip
```

## Running tests
Tests are written using [pytest](https://docs.pytest.org/).

```
cd <CHECKOUT>
python3 -m pytest tests/unit
```

Simply run the tests with pytest as a module when inside the root of the
checkout; this ensures the `target_postgres/` module directory is found on the
`PYTHONPATH`.
