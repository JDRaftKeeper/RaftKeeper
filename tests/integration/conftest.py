import logging

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--log", action="store", default="INFO", help="set logging level"
    )


@pytest.fixture(scope='session')
def logger():
    log_level = pytest.config.getoption("--log")
    logger = logging.getLogger('integration_test')

    numeric_level = getattr(
        logging,
        log_level.upper(),
        None
    )
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_level)

    logger.setLevel(numeric_level)
    return logger
