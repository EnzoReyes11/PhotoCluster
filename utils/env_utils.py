"""Utility functions for environment variable management."""

from __future__ import annotations

import os


def get_required_env_var(var_name: str, purpose: str | None = None) -> str:
    """Retrieve a required environment variable, raising ValueError if not found.

    Args:
        var_name: The name of the environment variable.
        purpose: An optional string describing the purpose of the environment
                 variable, to be included in the error message if not found.

    Returns:
        The value of the environment variable.

    Raises:
        ValueError: If the environment variable is not set or is an empty string.

    """
    value = os.getenv(var_name)
    if not value:
        if purpose:
            # Ensure the message for purpose does not exceed line length
            err_msg = (
                f"Environment variable '{var_name}' required for {purpose} is not"
                " set."
            )
            raise ValueError(err_msg)
        # Standard message if no purpose is provided
        err_msg = f"Environment variable '{var_name}' is required."
        raise ValueError(err_msg)
    return value
