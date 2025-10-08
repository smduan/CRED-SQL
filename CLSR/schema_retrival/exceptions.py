class RagBaseError(Exception):
    """
    Rag base exception that all Rag exceptions should inherit from.

    This error can be used to catch any Rag exceptions.
    """

    def __init__(self, message: str = ""):
        """
        Rag base exception initializer.

        Arguments:
            `message`:
                An error message specific to the context in which the error occurred.
        """

        self.message = message
        super().__init__(message)


class RagArgumentsError(RagBaseError):
    """Invalid Arguments."""


class RagTableAlreadyExistError(RagBaseError):
    """Same table already exists in region."""


class RagKeyReferenceMissingError(RagBaseError):
    """key  reference is missing."""


class RagDeleteSchemaError(RagBaseError):
    """delete schema failed."""
