#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Initializing functions for the scifi pipeline.
"""

import os
from typing import Dict, Any
from pathlib import Path
import logging
import pkg_resources

import yaml

try:
    # Even though there is no "scifi/_version" file,
    # it should be generated by
    # setuptools_scm when building the package
    from scifi._version import __version__
except ImportError:
    from setuptools_scm import get_version as _get_version

    __version__ = _get_version(root="..", relative_to=__file__)


def setup_logger(level="INFO", logfile=None) -> logging.Logger:
    """
    Set up a logger for the library.

    Parameters
    ----------

    level : :obj:`str`, optional
        Level of logging to display.
        See possible levels here:
        https://docs.python.org/2/library/logging.html#levels

        Defaults to "INFO".

    logfile : :obj:`str`, optional
        File to write log to.

        Defaults to "~/.scifi.log.txt".

    Returns
    -------
    :class:`logging.Logger`
        A logger called "scifi".
    """
    _LOGGER = logging.getLogger("scifi")
    _LOGGER.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    if logfile is None:
        logfile = Path("~/.scifi.log.txt").expanduser().absolute()

    try:
        logfile.touch()
    except (PermissionError, OSError):
        logfile = Path("./scifi.log.txt").absolute()
        try:
            logfile.touch()
        except (PermissionError, OSError):
            raise Exception(
                "Can't write logfile in eiher home directory or current directory."
            )

    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.getLevelName(level))
    # create formatter and add it to the handlers
    fmt = "scifi.v{}:%(module)s:L%(lineno)d ".format(__version__)
    fmt += "(%(funcName)s) [%(levelname)s] %(asctime)s > %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")
    fh.setFormatter(formatter)
    # fmt = "[%(levelname)s] > %(message)s"
    fmt = "scifi:%(module)s:L%(lineno)d (%(funcName)s) [%(levelname)s]"
    fmt += " > %(message)s"
    formatter = logging.Formatter(fmt)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    _LOGGER.addHandler(fh)
    _LOGGER.addHandler(ch)

    _LOGGER.debug(
        "This is scifi (https://github.com/epigen/scifiRNA-seq), version: {}".format(
            __version__
        )
    )
    return _LOGGER


def setup_config(custom_yaml_config=None) -> Dict[str, Any]:
    """
    Set up global library configuration.

    It reads scifi's package data to load a default configuration,
    tries to update it by reading a file in ``~/.scifi.config.yaml``
    if present, and lastly, updates it by reading a possible passed yaml file
    ``custom_yaml_config``.
    Non-exisiting fields will maintain the previous values, so that the user
    needs only to specify the section(s) as needed.

    Parameters
    ----------
    custom_yaml_config : :obj:`str`, optional
        Path to YAML file with configuration.
        To see the structure of the YAML file, see
        https://github.com/afrendeiro/toolkit/blob/master/scifi/config/default.yaml

        Defaults to :obj:`None`.

    Returns
    -------
    :obj:`dict`
        Dictionary with configurations.
    """
    default_config_path = "config/default.yaml"
    default_config_path = pkg_resources.resource_filename(
        __name__, default_config_path
    )
    _LOGGER.debug(
        "Reading default configuration file distributed"
        " with package from '{}'.".format(default_config_path)
    )
    try:
        _CONFIG = yaml.safe_load(open(default_config_path, "r"))
        _LOGGER.debug("Default config: {}".format(_CONFIG))
    except IOError:
        _LOGGER.error(
            "Couldn't read configuration file from '{}'.".format(
                default_config_path
            )
        )
        _CONFIG = dict()

    user_config_path = os.path.join(
        os.path.expanduser("~"), ".scifi.config.yaml"
    )
    if os.path.exists(user_config_path):
        # Read up
        _LOGGER.debug("Found custom user config: {}".format(user_config_path))
        try:
            custom_config = yaml.safe_load(open(user_config_path, "r"))
            _LOGGER.debug("Custom user config: {}".format(custom_config))
            # Update config
            _LOGGER.debug(
                "Updating configuration with custom file from '{}'.".format(
                    user_config_path
                )
            )
            _CONFIG.update(custom_config)
            _LOGGER.debug("Current config: {}".format(custom_config))
        except IOError:
            _LOGGER.error(
                "Configuration file from '{}' exists but is not readable."
                " Ignoring.".format(user_config_path)
            )
    else:
        _LOGGER.debug(
            "To use custom configurations including paths to static files,"
            " create a '{}' file.".format(user_config_path)
        )

    if custom_yaml_config is not None:
        # Read up
        try:
            custom_config = yaml.safe_load(open(custom_yaml_config, "r"))
            _LOGGER.debug("Custom passed config: {}".format(custom_config))
            # Update config
            _LOGGER.debug(
                "Updating configuration with custom file from '{}'.".format(
                    custom_yaml_config
                )
            )
            _CONFIG.update(custom_config)
            _LOGGER.debug("Current config: {}".format(custom_config))
        except IOError as e:
            _LOGGER.error(
                "Passed configuration from '{}' exists but is not readable.".format(
                    custom_yaml_config
                )
            )
            raise e

    return _CONFIG


_LOGGER = setup_logger()


if __name__ == "__main__":
    import sys
    from scifi.pipeline import main

    try:
        sys.exit(main())
    except KeyboardInterrupt:
        _LOGGER.error("Interrupted by user!")
        sys.exit(1)
