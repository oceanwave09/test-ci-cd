class Log4j(object):
    """Wrapper class for Log4j JVM object

    :param  spark: SparkSession object
    """

    def __init__(self, spark):
        # get spark app details from context
        conf = spark.sparkContext.getConf()
        app_id = conf.get("spark.app.id")
        app_name = conf.get("spark.app.name")

        # set spark "[app_name - app_id]" as log message prefix
        log4j = spark._jvm.org.apache.log4j
        message_prefix = "[" + app_name + " - " + app_id + "]"
        self._logger = log4j.LogManager.getLogger(message_prefix)
        self._logger.setLevel(log4j.Level.WARN)

    @property
    def logger(self):
        return self._logger

    def error(self, message):
        """Log a error message

        :param  message: Error message to write into log
        """
        self._logger.error(message)

    def warn(self, message):
        """Log a warning message

        :param  message: Warning message to write into log
        """
        self._logger.warn(message)

    def info(self, message):
        """Log a information message

        :param  message: Information message to write into log
        """
        self._logger.info(message)

    def debug(self, message):
        """Log a debug message

        :param  message: Debug message to write into log
        """
        self._logger.debug(message)
