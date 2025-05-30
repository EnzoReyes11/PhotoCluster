"""Utility functions for file system operations and path validations."""

from __future__ import annotations

from pathlib import Path

from .env_utils import get_required_env_var


def get_validated_path_from_env(
    var_name: str,
    purpose: str | None = None,
    *,  # Makes subsequent arguments keyword-only
    check_exists: bool = False,
    check_is_file: bool = False,
    check_is_dir: bool = False,
) -> Path:
    """Retrieve a path from an environment variable and perform specified validations.

    Args:
        var_name: The name of the environment variable.
        purpose: An optional string describing the purpose of the path,
                 to be included in error messages from get_required_env_var.
        check_exists: If True, ensures the path exists.
        check_is_file: If True, ensures the path exists and is a file.
        check_is_dir: If True, ensures the path exists and is a directory.

    Returns:
        A Path object representing the validated path.

    Raises:
        ValueError: If the environment variable is not set (from get_required_env_var).
        FileNotFoundError: If check_exists is True and the path does not exist,
                           or if check_is_file is True and the path is not a file.
        NotADirectoryError: If check_is_dir is True and the path is not a directory.

    """
    path_str = get_required_env_var(var_name, purpose)
    path_obj = Path(path_str)

    if check_exists and not path_obj.exists():
        err_msg = (
            f"Path from environment variable '{var_name}' ('{path_str}') does not"
            " exist."
        )
        raise FileNotFoundError(err_msg)

    if check_is_file:
        if not path_obj.exists():
            err_msg = (
                f"Expected a file at path from '{var_name}' ('{path_str}'), but it"
                " does not exist."
            )
            raise FileNotFoundError(err_msg)
        if not path_obj.is_file():
            err_msg = (
                f"Path from environment variable '{var_name}' ('{path_str}') is not a"
                f" file. It exists but is a directory or other type."
            )
            raise ValueError(err_msg)

    if check_is_dir:
        if not path_obj.exists():
            err_msg = (
                f"Expected a directory at path from '{var_name}' ('{path_str}'), but"
                " it does not exist."
            )

            # Using FileNotFoundError, even if what is not found is a directory.
            raise FileNotFoundError(err_msg)
        if not path_obj.is_dir():
            err_msg = (
                f"Path from environment variable '{var_name}' ('{path_str}') is not a"
                f" directory. It exists but is a file or other type."
            )
            raise NotADirectoryError(err_msg)

    return path_obj


def ensure_directory_exists(
    dir_path: Path,
    *,
    create_if_not_exists: bool = False,
) -> None:
    """Ensure a directory exists at the given path.

    Args:
        dir_path: The Path object representing the directory.
        create_if_not_exists: If True, the directory (and any necessary parents)
                              will be created if it doesn't exist.

    Raises:
        NotADirectoryError: If the path exists but is not a directory.
        FileNotFoundError: If the path does not exist and create_if_not_exists is False.

    """
    if dir_path.exists():
        if not dir_path.is_dir():
            err_msg = f"Path '{dir_path}' exists but is not a directory."
            raise NotADirectoryError(err_msg)
    elif create_if_not_exists:
        dir_path.mkdir(parents=True, exist_ok=True)
    else:
        err_msg = (
            f"Directory '{dir_path}' does not exist and create_if_not_exists is False."
        )
        raise FileNotFoundError(err_msg)
