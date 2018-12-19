def format_list(items, parenthesis=True):
    """
    Format an array into a prepared string that can be used in a SQL IN clause.

    Parameters
    ----------
    items : array_like
        A list of items for the prepared string.
    parenthesis : boolean, optional
        A flag indicating whether to wrap the prepared string in parenthesis
        (default: True).

    Returns
    -------
    str
        The prepared string.

    Raises
    ------
    ValueError
        If the are over 1000 items in the list.

    Examples
    --------
    >>> items = ['a', 'b', 'c']
    >>> format_list(items)
    "('a', 'b', 'c')"

    >>> format_list(items, parenthesis=False)
    "'a', 'b', 'c'"

    >>> items2 = [1, 2, 3, '4']
    >>> format_list(items2)
    "(1, 2, 3, '4')"
    """
    if len(items) > 1000:
        raise ValueError('Lists are limited to 1000 items.')
    formatted_list = ', '.join(
        [f"'{item}'" if isinstance(item, str) else str(item) for item in items])
    if parenthesis:
        return f"({formatted_list})"
    else:
        return formatted_list
