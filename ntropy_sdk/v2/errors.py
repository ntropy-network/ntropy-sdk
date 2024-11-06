class NtropyError(Exception):
    """A general error in the Ntropy SDK"""

    DESCRIPTION = "General error"

    def __str__(self):
        if len(self.args) == 1:
            details = self.args[0]
        else:
            details = self.args
        return f"{self.DESCRIPTION}: {details}"


class NtropyBatchError(Exception):
    """One or more errors in one or more transactions of a submitted transaction batch"""

    def __init__(self, message, id=None, errors=None):
        super().__init__(message)
        self.id = id
        self.errors = errors


class NtropyDatasourceError(Exception):
    """Errors in processing underlying document"""

    DESCRIPTION = "Error processing submitted document"

    def __init__(self, error_code=None, error=None):
        self.error_code = error_code or 500
        self.error = error or "Unknown Error"

    def __str__(self):
        return f"{self.DESCRIPTION}: {self.error_code}: {self.error}"


class NtropyBankStatementError(Exception):
    """Errors in processing underlying document"""

    DESCRIPTION = "Error processing submitted document"

    def __init__(self, id: str, code, message):
        self.id = id
        self.code = code
        self.message = message

    def __str__(self):
        return f"{self.DESCRIPTION}: {self.code}: {self.message}"


class NtropyTimeoutError(NtropyError):
    DESCRIPTION = "Operation timed out"


class NtropyHTTPError(NtropyError):
    """An expected error returned from the server-side"""

    def __init__(self, req_id=None, content=None):
        self.content = content
        self.req_id = req_id

    def __str__(self):
        if self.content is not None:
            if "details" in self.content:
                return f"Request {self.req_id}: {self.DESCRIPTION}: {self.content['details']}"
            elif "detail" in self.content:
                return f"Request {self.req_id}: {self.DESCRIPTION}: {self.content['detail']}"
        return f"Request {self.req_id}: {self.DESCRIPTION}"


class NtropyValidationError(NtropyHTTPError):
    DESCRIPTION = "One or more of the provided inputs is not a valid input"


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
        "Not enough credits to perform this operation. Please top up your account"
    )


class NtropyNotSupportedError(NtropyHTTPError):
    DESCRIPTION = "The requested operation is not supported for this API key. Please contact Ntropy support"


class NtropyResourceOccupiedError(NtropyHTTPError):
    DESCRIPTION = "The resource you're trying to access is busy or not ready yet"


class NtropyServerConnectionError(NtropyHTTPError):
    DESCRIPTION = "Server connection error. Please try again later."


class NtropyRateLimitError(NtropyHTTPError):
    DESCRIPTION = "Too many requests. Please try again later."


ERROR_MAP = {
    400: NtropyValueError,
    401: NtropyNotAuthorizedError,
    403: NtropyNotSupportedError,
    404: NtropyNotFoundError,
    409: NtropyResourceOccupiedError,
    422: NtropyValidationError,
    423: NtropyQuotaExceededError,
    500: NtropyRuntimeError,
    502: NtropyServerConnectionError,
    503: NtropyServerConnectionError,
    504: NtropyServerConnectionError,
}


def error_from_http_status_code(request_id: str, status_code: int, content: dict):
    ErrorClass = ERROR_MAP.get(status_code, NtropyHTTPError)
    return ErrorClass(request_id, content)
