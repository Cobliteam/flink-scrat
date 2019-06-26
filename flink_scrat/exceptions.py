
class MaxRetriesReachedException(Exception):
    pass


class FailedSavepointException(Exception):
    pass


class NotValidJARException(Exception):
    pass


class JobRunFailedException(Exception):
    pass


class JobIdNotFoundException(Exception):
    pass


class NoAppsFoundException(Exception):
    pass

class MultipleAppsFoundException(Exception):
    pass
