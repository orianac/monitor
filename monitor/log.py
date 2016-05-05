import logging
import logging.config


def set_logger(name='logname', loglvl='DEBUG'):
    """Set up logger"""

    loglvl_dict = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO,
                   'WARNING': logging.WARNING, 'ERROR': logging.ERROR,
                   'CRITICAL': logging.CRITICAL}
    logger = logging.getLogger(name)
    logger.setLevel(loglvl_dict[loglvl])
    ch = logging.StreamHandler()
    ch.setLevel(loglvl_dict[loglvl])
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s ' +
                                  '- %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger
