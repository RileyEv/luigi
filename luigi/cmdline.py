# @Author: Riley Evans
# @Date:   2018-08-02T11:39:56+01:00
# @Email:  revans35@jaguarlandrover.com
# @Project: DIY Report Automation
# @Filename: cmdline.py
# @Last modified by:   Riley Evans
# @Last modified time: 2018-08-07T11:17:06+01:00


import os
import argparse
import logging
import logging.config
import sys

from luigi.retcodes import run_with_retcodes


def luigi_run(argv=sys.argv[1:]):
    run_with_retcodes(argv)


def luigid(argv=sys.argv[1:]):
    import luigi.server
    import luigi.process
    import luigi.configuration
    parser = argparse.ArgumentParser(description=u'Central luigi server')
    parser.add_argument(
        u'--background', help=u'Run in background mode', action='store_true')
    parser.add_argument(u'--pidfile', help=u'Write pidfile')
    parser.add_argument(u'--logdir', help=u'log directory')
    parser.add_argument(u'--state-path', help=u'Pickled state file')
    parser.add_argument(u'--address', help=u'Listening interface')
    parser.add_argument(u'--unix-socket', help=u'Unix socket path')
    parser.add_argument(u'--port', default=8082, help=u'Listening port')

    opts = parser.parse_args(argv)

    if opts.state_path:
        config = luigi.configuration.get_config()
        config.set('scheduler', 'state_path', opts.state_path)

    if opts.background:
        # daemonize sets up logging to spooled log files
        logging.getLogger().setLevel(logging.INFO)
        luigi.process.daemonize(luigi.server.run, api_port=opts.port,
                                address=opts.address, pidfile=opts.pidfile,
                                logdir=opts.logdir, unix_socket=opts.unix_socket)
    else:
        if opts.logdir:
            logging.basicConfig(level=logging.INFO, format=luigi.process.get_log_format(),
                                filename=os.path.join(opts.logdir, "luigi-server.log"))
        else:
            config = luigi.configuration.get_config()
            logging_conf = None
            if not config.getboolean('core', 'no_configure_logging', False):
                logging_conf = config.get('core', 'logging_conf_file', None)
                if logging_conf is not None and not os.path.exists(logging_conf):
                    raise Exception(
                        "Error: Unable to locate specified logging configuration file!")
            if logging_conf is not None:
                print("Configuring logging from file: {}".format(logging_conf))
                logging.config.fileConfig(logging_conf)
            else:
                print(
                    "Defaulting to basic logging; consider specifying logging_conf_file in luigi.cfg.")
                logging.basicConfig(level=logging.INFO,
                                    format=luigi.process.get_log_format())
        luigi.server.run(api_port=opts.port, address=opts.address,
                         unix_socket=opts.unix_socket)


# Luigi Worker cmd tool

# def luigiw(argv=sys.argv[1:]):
#     import luigi.server
#     import luigi.process
#     import luigi.configuration
#     parser = argparse.ArgumentParser(description=u'Luigi namespace worker')
#     parser.add_argument(
#         u'--background', help=u'Run in background mode', action='store_true')
#     parser.add_argument(u'--pidfile', help=u'Write pidfile')
#     parser.add_argument(u'--logdir', help=u'log directory')
#     parser.add_argument(u'--state-path', help=u'Pickled state file')
#     parser.add_argument(u'--address', help=u'Listening interface')
#     parser.add_argument(u'--unix-socket', help=u'Unix socket path')
#     parser.add_argument(u'--port', default=8082, help=u'Listening port')
#
#     opts = parser.parse_args(argv)
#
#     if opts.state_path:
#         config = luigi.configuration.get_config()
#         config.set('scheduler', 'state_path', opts.state_path)
#
#     if opts.background:
#         # daemonize sets up logging to spooled log files
#         logging.getLogger().setLevel(logging.INFO)
#         luigi.process.daemonize(luigi.server.run, api_port=opts.port,
#                                 address=opts.address, pidfile=opts.pidfile,
#                                 logdir=opts.logdir, unix_socket=opts.unix_socket)
#     else:
#         if opts.logdir:
#             logging.basicConfig(level=logging.INFO, format=luigi.process.get_log_format(),
#                                 filename=os.path.join(opts.logdir, "luigi-server.log"))
#         else:
#             config = luigi.configuration.get_config()
#             logging_conf = None
#             if not config.getboolean('core', 'no_configure_logging', False):
#                 logging_conf = config.get('core', 'logging_conf_file', None)
#                 if logging_conf is not None and not os.path.exists(logging_conf):
#                     raise Exception(
#                         "Error: Unable to locate specified logging configuration file!")
#             if logging_conf is not None:
#                 print("Configuring logging from file: {}".format(logging_conf))
#                 logging.config.fileConfig(logging_conf)
#             else:
#                 print(
#                     "Defaulting to basic logging; consider specifying logging_conf_file in luigi.cfg.")
#                 logging.basicConfig(level=logging.INFO,
#                                     format=luigi.process.get_log_format())
#         luigi.server.run(api_port=opts.port, address=opts.address,
#                          unix_socket=opts.unix_socket)


def luigiw(argv=sys.argv[1:]):
    """
    A luigi worker which runs tasks with the given namespace
    """
    logger = logging.getLogger('luigi-interface')
    with luigi.cmdline_parser.CmdlineParser.global_instance(argv):
        retcodes = retcode()

    worker = None
    try:
        worker = luigi.interface._run(argv)['worker']
    except luigi.interface.PidLockAlreadyTakenExit:
        sys.exit(retcodes.already_running)
    except Exception:
        # Some errors occur before logging is set up, we set it up now
        luigi.interface.setup_interface_logging()
        logger.exception("Uncaught exception in luigi")
        sys.exit(retcodes.unhandled_exception)

    # with luigi.cmdline_parser.CmdlineParser.global_instance(argv):
    #     task_sets = luigi.execution_summary._summary_dict(worker)
    #     root_task = luigi.execution_summary._root_task(worker)
    #     non_empty_categories = {k: v for k, v in task_sets.items() if v}.keys()
