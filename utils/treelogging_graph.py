# import built-in packages
from io import BytesIO
# import 3rd party packages
import pytz
import pandas
from matplotlib import pyplot as plt

async def util_graph_summary(
    df: pandas.DataFrame,
    max_duration: int,
    output_timezone: pytz.timezone
) -> BytesIO:
    """
    Docstring for util_graph_summary
    
    :param df: Pandas dataframe containing the columns 'start' and 'end'
    :type df: pandas.DataFrame
    :param max_duration: The maximum non-outlier value (seconds)
    :type max_duration: int
    :param output_timezone: The timezone which the values will be converted to
    :type output_timezone: pytz.timezone
    :return: BytesIO containing a PNG image of the graph
    :rtype: BytesIO
    
    """
    # create a copy
    df = df.copy()

    # calculate uptime and downtime
    df['uptime']   = df['end'] - df['start']
    df['downtime'] = df['start'] - df['end'].shift(1)

    df['uptime']    = df['uptime'].dt.total_seconds()
    df['downtime']  = df['downtime'].dt.total_seconds()

    df['ratio'] = df['downtime'] / df['uptime']

    # remove values that are too large
    df = df[(df['downtime'] < max_duration) & (df['uptime'] < max_duration)]

    # convert timezone AFTER calculations
    df[['start', 'end']] = df[['start', 'end']].apply(
        lambda col: col.dt.tz_convert(output_timezone)
    )

    # create columns for hour/day
    df['hour_of_day'] = df['end'].dt.hour
    df['day_of_week'] = df['end'].dt.day_of_week
    df['date']        = df['end'].dt.date

    # drop na from dataframe
    df = df.dropna()

    # group by
    hourly_downtime = df.groupby('hour_of_day')['downtime']
    daily_downtime  = df.groupby('day_of_week')['downtime']

    # calculate averages for hour/day
    hourly_med = hourly_downtime.median()
    daily_med  = daily_downtime.median()
    date_ratio   = df.groupby('date')['ratio'].mean()

    # set figure size
    # https://matplotlib.org/stable/users/explain/axes/arranging_axes.html#manual-adjustments-to-a-gridspec-layout
    fig = plt.figure(figsize=(16, 9), layout="constrained")
    spec = fig.add_gridspec(nrows=2, ncols=2)

    # plot hour_of_day
    ax0 = fig.add_subplot(spec[0, 0])
    # main median line
    ax0.plot(
        hourly_med.index, hourly_med,
        '-', color='mediumseagreen', linewidth=2
    )
    # shaded region of different widths ig
    for lower, upper, alpha in (
        (40, 60, 0.3),
        (25, 75, 0.2),
        (10, 90, 0.1),
    ):
        hourly_lower = hourly_downtime.quantile(lower / 100)
        hourly_upper = hourly_downtime.quantile(upper / 100)
        ax0.fill_between(
            hourly_med.index, hourly_lower, hourly_upper,
            color='mediumseagreen', alpha=alpha,
            label=f'{lower}th-{upper}th Percentile'
        )

    # add labels
    ax0.set_title('Median Watering Downtime by Hour of the Day')
    ax0.set_xlabel('Hour of the Day')
    ax0.set_ylabel('Watering Downtime (seconds)')
    ax0.set_xticks(hourly_med.index)
    ax0.grid(True, linestyle='-', alpha=0.2)
    ax0.legend(loc='upper center', ncol=3)

    # plot day_of_week
    ax1 = fig.add_subplot(spec[0, 1])
    # main median line
    ax1.plot(
        daily_med.index, daily_med,
        '-', color='forestgreen', linewidth=2
    )
    # shaded region of different widths ig
    for lower, upper, alpha in (
        (40, 60, 0.3),
        (25, 75, 0.2),
        (10, 90, 0.1),
    ):
        daily_lower = daily_downtime.quantile(lower / 100)
        daily_upper = daily_downtime.quantile(upper / 100)
        ax1.fill_between(
            daily_med.index, daily_lower, daily_upper,
            color='forestgreen', alpha=alpha,
            label=f'{lower}th-{upper}th Percentile'
        )

    # add labels
    ax1.set_title('Median Watering Downtime by Day of the Week')
    ax1.set_xlabel('Day of the Week')
    ax1.set_ylabel('Watering Downtime (seconds)')
    ax1.set_xticks(daily_med.index)
    ax1.grid(True, linestyle='-', alpha=0.2)
    ax1.legend(loc='upper center', ncol=3)

    # plot downtime by date - line chart with error bars
    ax2 = fig.add_subplot(spec[1, :])
    ax2.bar(
        date_ratio.index, date_ratio,
        color='darkolivegreen',
        label='Average Downtime'
    )

    # add labels
    ax2.set_title('Average Watering Ratio Timeline')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Avg Downtime / Avg Uptime')
    ax2.set_xticks(date_ratio.index)
    ax2.tick_params(axis='x', rotation=90)
    ax2.grid(True, linestyle='-', alpha=0.2)

    # save to a BytesIO buffer
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    plt.close()
    # return the buffer
    buffer.seek(0)
    return buffer
