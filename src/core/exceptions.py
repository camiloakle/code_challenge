"""Domain and pipeline exceptions."""


class BillupsError(Exception):
    """Base error for the application."""

    pass


class DataValidationError(BillupsError):
    """Raised when input data fails validation rules."""

    pass


class PipelineExecutionError(BillupsError):
    """Raised when a pipeline step cannot complete."""

    pass


class StorageError(BillupsError):
    """Raised when reading or writing storage fails."""

    pass
