class NtropyError(Exception):
    """A general error in the Ntropy SDK"""

    DESCRIPTION = "General error"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __str__(self):
        if len(self.args) == 1:
            details = self.args[0]
        else:
            details = self.args
        return f"{self.DESCRIPTION}: {details}"


class NtropyBatchError(Exception):
    """One or more errors in one or more transactions of a submitted transaction batch"""

    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class NtropyTimeoutError(NtropyError):
    DESCRIPTION = "Operation timed out"


class NtropyModelTrainingError(NtropyError):
    DESCRIPTION = "Error during model training"


class NtropyHTTPError(NtropyError):
    """An expected error returned from the server-side"""

    def __init__(self, content=None):
        self.content = content

    def __str__(self):
        if self.content is not None and "details" in self.content:
            return f"{self.DESCRIPTION}: {self.content['details']}"
        return self.DESCRIPTION


class NtropyValidationError(NtropyHTTPError):
    DESCRIPTION = "Failed to authorize the client using the provided API key"


class NtropyNotAuthorizedError(NtropyHTTPError):
    DESCRIPTION = "Failed to authorize the client using the provided API key"


class NtropyRuntimeError(NtropyHTTPError):
    DESCRIPTION = "Unexpected error on the server during execution of your request"


class NtropyNotFoundError(NtropyHTTPError):
    DESCRIPTION = "Requested resource was not found"


class NtropyValueError(NtropyHTTPError):
    DESCRIPTION = "One or more values in the provided request are invalid"


class NtropyQuotaExceededError(NtropyHTTPError):
    DESCRIPTION = (
        "Reached the transaction limit for this API key. Please contact Ntropy support"
    )


class NtropyNotSupportedError(NtropyHTTPError):
    DESCRIPTION = "The requested operation is not support for this API key. Please contact Ntropy support"


class NtropyResourceOccupiedError(NtropyHTTPError):
    DESCRIPTION = "The resource you're trying to access is busy or not ready yet"


ERROR_MAP = {
    400: NtropyValueError,
    401: NtropyNotAuthorizedError,
    403: NtropyNotSupportedError,
    404: NtropyNotFoundError,
    409: NtropyResourceOccupiedError,
    422: NtropyValidationError,
    423: NtropyQuotaExceededError,
    500: NtropyRuntimeError,
}


def error_from_http_status_code(status_code: int, content: dict):
    ErrorClass = ERROR_MAP.get(status_code, NtropyError)
    return ErrorClass(content)
