import sys
import os.path

import ecflow

from monitor.log import set_logger


# -------------------------------------------------------------------- #
def main():
    """Start ECFLOW from HALTED server.

    Delete any currently loaded suite definitions and tasks. Load
    a new suite definition file. Restart the server. Run new suite. Run
    this version when starting with a HALTED server. Run from inside
    def_files directory."""

    logger = set_logger(os.path.splitext(os.path.split(__file__)[-1])[0],
                        'INFO')

    if len(sys.argv) == 2:
        suite = sys.argv[1]
        def_file = suite + ".def"
    else:
        logger.critical('Usage: python {} suite_name'.format(__file__))
        sys.exit()

    try:
        logger.info('Loading definition in '+def_file+' into the server')
        ci = ecflow.Client()
        ci.delete_all()
        # Read definition from disk and load into the server.
        ci.load(def_file)

        logger.info('Restarting the server. This starts job scheduling')
        ci.restart_server()

        logger.info('Begin the suite named ' + suite)
        ci.begin_suite(suite)
    except RuntimeError:
        msg = ('Error in {}\n'.format(__file__) +
               'ecflow was not able to begin suite {}'.format(suite))
        if sys.version_info[0] != 3:
            import traceback
            msg += '\n\nOriginal traceback:\n' + traceback.format_exc()
        raise RuntimeError(msg)
# -------------------------------------------------------------------- #


# -------------------------------------------------------------------- #
if __name__ == "__main__":
    main()
# -------------------------------------------------------------------- #
