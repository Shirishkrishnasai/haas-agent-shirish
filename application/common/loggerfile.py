import glob
import logging
import logging.handlers
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
LOG_FILENAME = '/opt/agent/logging_rotatingfile_example.out'
    # Set up a specific logger with our desired output level
my_logger = logging.getLogger('MyLogger')
my_logger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=10000, backupCount=5)

my_logger.addHandler(handler)
#Log some messages
#for i in range(20):
 #       my_logger.debug('i = %d' % i)

# See what files are created
logfiles = glob.glob('%s*' % LOG_FILENAME)

#for filename in logfiles:
 #       my_logger.debug(filename)

