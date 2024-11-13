import pandas as pd

# Get the log streams, handling pagination using nextToken
def get_all_log_streams(cloudwatch, log_group_name):
    log_streams = []
    next_token = None
    
    while True:
        if next_token:
            response = cloudwatch.describe_log_streams(
                logGroupName=log_group_name,
                nextToken=next_token
            )
        else:
            response = cloudwatch.describe_log_streams(
                logGroupName=log_group_name
            )

        log_streams.extend(response.get('logStreams', []))
        
        # Check if there's a next token, if not, break the loop
        next_token = response.get('nextToken')
        if not next_token:
            break
    
    return log_streams


def update_logs(cloudwatch, log_groups, verbose=False):
    for k in log_groups:
        # Extract the log group name from the dictionary
        log_group_name = log_groups[k]['log_group_name']

        if verbose:
            print(log_group_name)

        # Add the strem information to the log groups dictionary
        log_groups[k]['log_streams'] = get_all_log_streams(cloudwatch, log_group_name)

    # List all groups and all streams
    for group in log_groups:

        if verbose:
            print(group)

        # Create an empty list to store the log group events df
        events_logs_dfs = []

        for stream in log_groups[group]['log_streams']:

            # Retrieve log events for the selected log group and stream
            log_err, events = get_events(
                cloudwatch,
                log_groups[group]['log_group_name'],
                stream['logStreamName']
                )
            
            # Add the events to the df list
            if log_err == 0:
                events_logs_dfs.append(pd.DataFrame(events)[['timestamp', 'message']])
                events_logs_dfs[-1]['stream'] = stream['logStreamName']  # include stream name

        if verbose:
            print(len(events_logs_dfs), 'streams found.')

        # Add events as a single DataFrame to the log group dictionary
        log_groups[group]['events'] = pd.concat(events_logs_dfs).reset_index(drop=True)

    return log_groups


def log_groups_stats(log_groups):
    # Create an empty list to store the information
    logs_info = []

    for k in log_groups:
        logs_info.append(
            {
                'feature': k,
                'log_group': log_groups[k]['log_group_name'],
                'nb_streams': len(log_groups[k]['log_streams'])
            }
        )

    # Store info in DataFrame
    logs_info_df = pd.DataFrame(logs_info)

    # Get the latest stream name for each feature
    logs_info_df['latest_stream'] = logs_info_df['feature'].apply(lambda x: get_latest_stream(log_groups, x))

    return logs_info_df


def get_latest_stream(log_groups: dict, feature: str):
    '''
    Return the latest stream for a given feature, based on the timestamp
    '''
    # Get all streams in a DataFrame
    streams_df = pd.DataFrame(log_groups[feature]['log_streams'])

    # Return the stream name for the most recent (max) timestamp
    return streams_df.loc[streams_df['lastEventTimestamp'] == streams_df['lastEventTimestamp'].max()]['stream_name'].values[0]

def get_events(cloudwatch, log_group_name, log_stream_name):
    '''
    Start CloudWatch Logs session and retrieve log events from the stream
    '''

    # Get log stream events
    events = cloudwatch.get_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        startFromHead=True  # Set to True to get log events in the order they occurred
    )

    # Get status code from API request
    status = events['ResponseMetadata']['HTTPStatusCode']

    if status == 200:
        return (0, events['events'])
    else:
        return (-1, f"Request returned with status code {status}")
    
def log_events(cloudwatch, logs_info_df, feature: str):
    '''
    Retrieve the log events for a specific feature
    '''

    # Get the log group and latest stream for the selected feature
    log_group_name = logs_info_df.loc[logs_info_df['feature']==feature, 'log_group'].values[0]
    log_stream_name = logs_info_df.loc[logs_info_df['feature']==feature, 'latest_stream'].values[0]

    # Retrieve log events for the selected log group and stream
    log_err, events = get_events(cloudwatch, log_group_name, log_stream_name)

    if log_err:
        # Print error message if applicable
        print(events)
        return pd.DataFrame()
    else:
        # Save events in a DataFrame
        events_df = pd.DataFrame(events)

        # Convert timestamp to datetime
        events_df['timestamp'] = pd.to_datetime(events_df['timestamp'], unit='ms')

        return events_df[['timestamp', 'message']]