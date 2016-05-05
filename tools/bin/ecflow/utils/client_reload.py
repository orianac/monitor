import sys
import os.path

import ecflow

from monitor.log import set_logger


# -------------------------------------------------------------------- #
def main():
    """Replace definition file and run new suite.

    Delete any currently loaded definition files and tasks. Load the
    suite definition file, and run the suite.
    Run this version to start when you are starting with a RUNNING server.
    Run from inside the def_files/ directory.
    """

    logger = set_logger(os.path.splitext(os.path.split(__file__)[-1])[0],
                        'INFO')

    logger.info('Client -> Server: delete, then load a new definition')

    if len(sys.argv) == 2:
        suite = sys.argv[1]
        def_file = suite + '.def'
    else:
        logger.critical('Usage: python {} suite_name'.format(__file__))
        sys.exit()

    try:
        ci = ecflow.Client()
        # Clear out the server
        ci.delete_all()
        # Load the definition into the server.
        ci.load(def_file)
        # Start the suite.
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
