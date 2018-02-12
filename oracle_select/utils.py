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


def process_monitor(db, processes, freq=5, timeout=60*60*4, on_success=None, 
                  on_failure=None):
    """
    Monitor PeopleSoft Process Scheduler instances.
    
    This function will poll the database for the processes given once every 
    ``freq`` seconds until the timeout is reached. Optional callbacks will be 
    called when the appropriate conditions are reached.
    
    Parameters
    ----------
    db : oracle_select.DB
        An oracle_select DB instance which will be used to connect to the 
        database.
    freq : int
        The frequency (in seconds) to poll the process monitor tables.
    timeout : int
        The maximum time (in seconds) to poll the database before raising a 
        TimeOutError.
    on_success : func (optional)
        Callback function that will be called when all processes reach a 
        finished (success) status. The callback should accept a pandas.DataFrame
        consisting of all processes and their attributes.
    on_failure : func (optional)
        Callback function that will be called if any processes reach a 
        failed (error, cancelled) status. The callback should accept a 
        pandas.DataFrame consisting of all processes and their attributes.
        
    Returns
    -------
    processes : pandas.DataFrame
        A DataFrame of the processes that were polled and their attributes.
        
    Raises
    ------
    Exception
        If a job fails.
    TimeOutError
        If the timeout is reached but the jobs have not yet finished.
    """
    from IPython import display
    import time
    
    WORKING_STATUSES = [5, 6, 7, 11, 14, 15, 16, 18]
    COMPLETE_STATUSES = [9]
    
    def set_status(code):
        code = int(code)
        if code in COMPLETE_STATUSES:
            return 'Finished'
        elif code in WORKING_STATUSES:
            return 'Working'
        else:
            return 'Failed'
    
    if isinstance(processes, (str, int)):
            processes = [processes]

    sql = """
        select
            prcsinstance,
            prcsname,
            prcstype,
            runcntlid,
            runstatus,
            begindttm,
            enddttm
        from ps_pmn_prcslist
        where
            prcsinstance in ({})
        order by prcsinstance"""
    prepared_sql = sql.format(', '.join(
        [f"'{prcs}'" for prcs in processes]))
    start = time.time()

    while time.time() - start <= timeout:
        display.clear_output(wait=True)
        df = pd.DataFrame(db.select(prepared_sql))
        df['status'] = df.runstatus.apply(set_status)

        msg = "{time}: {status}"

        if (df.status == 'Finished').all():
            status = 'Complete'
            print(msg.format(time=time.strftime('%I:%M:%S %p'), status=status))
            if on_success:
                on_success(df)
            return df
        elif (df.status == 'Failed').any():
            status = 'Failed'
            print(msg.format(time=time.strftime('%I:%M:%S %p'), status=status))
            if on_failure:
                on_failure(df)
            raise Exception('A job failure was detected.')
        elif (df.status == 'Working').any():
            status = 'Working'
            print(msg.format(time=time.strftime('%I:%M:%S %p'), status=status))
            display.display(df)
            time.sleep(freq)
        else:
            raise Exception('Something went wrong.')
    raise TimeOutError('Timeout reached.')
