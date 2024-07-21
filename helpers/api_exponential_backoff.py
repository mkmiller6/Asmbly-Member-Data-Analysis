def backoff_time(retry_count: int) -> float:
    """
    Calculates the backoff time for retrying an operation. The backoff time increases exponentially.

    Parameters:
        retry_count (int): The number of times the operation has been retried.

    Returns:
        wait_time (float): The calculated backoff time in seconds.
    """
    initial_delay = 200 / 1000
    wait_time = (2**retry_count) * initial_delay
    return wait_time
