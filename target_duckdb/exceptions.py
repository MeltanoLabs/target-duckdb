class InvalidSingerMessageError(Exception):
    """Raised when the Singer message is invalid."""


class SingerMessagesOutOfOrderError(Exception):
    """Raised when the Singer messages are not in the correct order."""


class ConfigError(Exception):
    """Raised when the configuration is invalid."""
