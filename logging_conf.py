LOG_CONFIG = {
    "version": 1,
    "root": {
        "handlers": ["console", "file"],
        "level": "DEBUG"
    },
    "handlers": {
        "console": {
            "formatter": "std_out",
            "class": "logging.StreamHandler",
            "level": "DEBUG"
        },
        "file": {
            "level": "INFO",
            "class": "logging.FileHandler",
            "filename": "log.log",
            "formatter": "fileformat",
        },
    },
    "formatters": {
        "std_out": {
            "format": "%(asctime)s : %(levelname)s  %(module)s : %(lineno)d - %(message)s",
            "datefmt":"%d-%m-%Y %I:%M:%S"

        },
        "fileformat": {
            "format": "%(asctime)s : %(levelname)s  %(module)s  %(funcName)s : %(lineno)d - %(message)s",
            "datefmt":"%d-%m-%Y %I:%M:%S"
        }
    },
}
