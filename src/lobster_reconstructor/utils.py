def format_timestamp(seconds_from_midnight: float, display_micro=False) -> str:
    """
    Formats a timestamp in seconds from midnight into a human-readable string.

    Parameters
    ----------
    seconds_from_midnight : float
        The timestamp in seconds, measured from midnight (00:00:00).
    display_micro : bool, optional
        If True, the output string will include microseconds. Defaults to False.

    Returns
    -------
    str
        A formatted string representing the time in HH:MM:SS or HH:MM:SS.ffffff format.
    """
    hours = int(seconds_from_midnight // 3600)
    mins = int((seconds_from_midnight % 3600) // 60)
    secs = int(seconds_from_midnight % 60)
    microsecs = int((seconds_from_midnight - int(seconds_from_midnight)) * 1_000_000)
    return (
        f"{hours:02d}:{mins:02d}:{secs:02d}.{microsecs:06d}"
        if display_micro
        else f"{hours:02d}:{mins:02d}:{secs:02d}"
    )

# def scale_format_price(price: int, price_scaling: float) -> str:
