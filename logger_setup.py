import logging

# Logger
class LoggerSetup:
    def __init__(self, path):
        self.logger = logging.getLogger('open_interest_logger')
        self.logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler(path, mode='a')
        file_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger