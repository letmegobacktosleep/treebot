# import built-in packages
import pytz
from io import BytesIO
# import 3rd party packages
import pandas
from matplotlib import pyplot as plt

async def util_graph_summary(
    df: pandas.DataFrame,
    max_duration: int,
    output_timezone: pytz.timezone
):
    """
    Args:
        df: a pandas dataframe containing two columns of datetime strings, 'start' and 'end'
        max_duration: maximum difference between 'start' and 'end' before it is treated as an outlier
        output_timezone: what timezone to convert the values to
    Returns:
        a io.BytesIO object representing a png of the graph
    """
    # convert timezones
    df[['start', 'end']] = df[['start', 'end']].apply(
        lambda col: col.dt.tz_convert(output_timezone)
    )

    # calculate uptime and downtime
    df['uptime']   = df['end'] - df['start']
    df['downtime'] = df['start'] - df['end'].shift(1)

    df['uptime']    = df['uptime'].dt.total_seconds()
    df['downtime']  = df['downtime'].dt.total_seconds()

    df['ratio'] = df['downtime'] / df['uptime']

    # remove values that are too large
    df = df[(df['downtime'] < max_duration) & (df['uptime'] < max_duration)]
    df = df.dropna()

    # create columns for hour/day
    df['hour_of_day'] = df['end'].dt.hour
    df['day_of_week'] = df['end'].dt.day_of_week
    df['date']        = df['end'].dt.date

    # calculate averages for hour/day
    hourly_avg = df.groupby('hour_of_day')['downtime'].mean()
    daily_avg  = df.groupby('day_of_week')['downtime'].mean()
    date_ratio   = df.groupby('date')['ratio'].mean().dropna()

    # calculate stdev for hour/day
    hourly_std = df.groupby('hour_of_day')['downtime'].std().fillna(0)
    daily_std  = df.groupby('day_of_week')['downtime'].std().fillna(0)

    # set figure size
    # https://matplotlib.org/stable/users/explain/axes/arranging_axes.html#manual-adjustments-to-a-gridspec-layout
    fig = plt.figure(figsize=(16, 9), layout="constrained")
    spec = fig.add_gridspec(nrows=2, ncols=2)

    # plot hour_of_day - line chart with error bars
    ax0 = fig.add_subplot(spec[0, 0])
    ax0.errorbar(
        hourly_avg.index, hourly_avg, yerr=hourly_std,
        fmt='-o', color='mediumseagreen',
        ecolor='darkgrey', elinewidth=1,
        label='Average Downtime',
        capsize=5
    )

    # add labels
    ax0.set_title('Average Watering Downtime by Hour of the Day')
    ax0.set_xlabel('Hour of the Day')
    ax0.set_ylabel('Average Watering Downtime (seconds)')
    ax0.set_xticks(hourly_avg.index)
    ax0.grid(True, linestyle='-', alpha=0.2)
    ax0.legend()

    # plot day_of_week - line chart with error bars
    ax1 = fig.add_subplot(spec[0, 1])
    ax1.errorbar(
        daily_avg.index, daily_avg, yerr=daily_std,
        fmt='-o', color='forestgreen',
        ecolor='darkgrey', elinewidth=1,
        label='Average Downtime',
        capsize=5
    )

    # add labels
    ax1.set_title('Average Watering Downtime by Day of the Week')
    ax1.set_xlabel('Day of the Week')
    ax1.set_ylabel('Average Watering Downtime (seconds)')
    ax1.set_xticks(daily_avg.index)
    ax1.grid(True, linestyle='-', alpha=0.2)
    ax1.legend()

    # plot downtime by date - line chart with error bars
    ax2 = fig.add_subplot(spec[1, :])
    ax2.bar(
        date_ratio.index, date_ratio,
        color='darkolivegreen',
        label='Average Downtime'
    )

    # add labels
    ax2.set_title('Watering Ratio Timeline')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Watering Ratio (downtime/uptime)')
    ax2.set_xticks(date_ratio.index)
    ax2.tick_params(axis='x', rotation=90)
    ax2.grid(True, linestyle='-', alpha=0.2)
    ax2.legend()

    # save to a BytesIO buffer
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    plt.close()
    # return the buffer
    buffer.seek(0)
    return buffer
