import asyncio
from pathlib import Path
from datetime import datetime, timedelta
import pytz
import pandas
from utils.constants import DATETIME_STRING_FORMAT

class TreeLogFile:
    """
    Manages the CSV logs with mutex
    """

    def __init__(self, directory: str = "data") -> None:
        self.dir = Path(directory)
        self.mutex: dict[int, asyncio.Lock] = {}
        self.loaded = False

    async def load_logs(
        self,
        guild_ids: list[int]
    ) -> None:
        """
        Creates an asyncio.Lock() for every guild the bot is in
        
        :param guild_ids: A list of guild IDs to create a mutex lock for
        :type guild_ids: list[int]
        """
        for guild_id in guild_ids:
            # create the mutex lock
            self.mutex.setdefault(guild_id, asyncio.Lock())
            # ensure the logs exist
            log_path = self.dir.joinpath(f"{guild_id}.csv")
            if not log_path.exists():
                df = pandas.DataFrame(
                    columns=['start', 'end', 'type']
                )
                await asyncio.to_thread(
                    lambda log_path=log_path, df=df: df.to_csv(
                        log_path, index=False,
                        encoding="utf-8"
                    )
                )
        # signal that loading is finished
        self.loaded = True

    def read_log_chunked(
        self,
        log_path: Path,
        start: datetime,
        end: datetime,
        filter_logs: tuple[str, ...]
    ) -> pandas.DataFrame:
        """
        Reads the logs in chunks and returns the logs within the specified interval

        :param log_path: Description
        :type log_path: Path
        :param start: The earliest timestamp that you want to fetch
        :type start: datetime
        :param end: The latest timestamp that you want to fetch
        :type end: datetime
        :param filter_logs: The log types which you want to fetch
        :type filter_logs: tuple[str, ...] | None
        :return: Pandas dataframe containing the logs
        :rtype: DataFrame
        """
        # iterate through the csv file in chunks
        chunks = []
        for chunk in pandas.read_csv(
            log_path,
            chunksize=10000,
            parse_dates=['start', 'end'],
            date_format=DATETIME_STRING_FORMAT
        ):
            # filter by log type
            if filter_logs is not None:
                chunk = chunk[chunk['type'].isin(filter_logs)]

            # set timezone as UTC
            chunk[['start', 'end']] = chunk[['start', 'end']].apply(
                lambda col: col.dt.tz_localize(pytz.utc)
            )

            # filter within the specified interval
            chunk = chunk[(chunk['end'] >= start) & (chunk['start'] <= end)]

            # add the chunk to the list
            chunks.append(chunk)

        if chunks:
            return pandas.concat(chunks, ignore_index=True)
        else:
            return pandas.DataFrame()

    async def read_log(
        self,
        guild_id: int,
        start: datetime = datetime.now(tz=pytz.utc) - timedelta(days=1),
        end: datetime = datetime.now(tz=pytz.utc),
        filter_logs: tuple[str, ...] | None = ('water',)
    ) -> pandas.DataFrame:
        """
        Returns the logs within the specified interval
        
        :param guild_id: The guild ID of the guild you want to fetch the logs for
        :type guild_id: int
        :param start: The earliest timestamp that you want to fetch
        :type start: datetime
        :param end: The latest timestamp that you want to fetch
        :type end: datetime
        :param filter_logs: The log types which you want to fetch
        :type filter_logs: tuple[str, ...] | None
        :return: Pandas dataframe containing the logs
        :rtype: DataFrame
        """
        log_path = self.dir.joinpath(f"{guild_id}.csv")
        # ignore if the log path does not exist
        if not log_path.exists():
            return None

        # wait until logs are loaded
        while not self.loaded:
            await asyncio.sleep(1)

        # read the csv log
        async with self.mutex[guild_id]:
            df = await asyncio.to_thread(
                lambda
                log_path=log_path,
                start=start, end=end,
                filter_logs=filter_logs:
                    self.read_log_chunked(
                        log_path=log_path,
                        start=start, end=end,
                        filter_logs=filter_logs
                    )
            )

        def remove_overlaps(group: pandas.DataFrame) -> pandas.DataFrame:
            """
            overlaps may occur when on_raw_message_edit is not processed fast enough
            this often occurs when the bot is starting up, since processing is deferred
            until after the config is loaded
            
            :param group: Description
            :type group: pandas.DataFrame
            :return: Description
            :rtype: DataFrame
            """
            # first row is always valid
            valid_rows = [True]
            prev_end = group.iloc[0]['end']
            # iterate through group
            for idx in range(1, len(group)):
                current_start = group.iloc[idx]['start']
                # only keep rows where (start) >= (previous end)
                if current_start >= prev_end:
                    valid_rows.append(True)
                    prev_end = group.iloc[idx]['end']
                else:
                    # row overlaps, skip row
                    valid_rows.append(False)
            # return the valid rows
            return group[valid_rows]

        # set timezone as UTC
        # df[['start', 'end']] = df[['start', 'end']].apply(
        #     lambda col: col.dt.tz_localize(pytz.utc)
        # )

        # filter the log type
        # if filter_logs is not None:
        #     df = df[df['type'].isin(filter_logs)]

        # filter within the specified interval
        # df = df[(df['end'] >= start) & (df['start'] <= end)]

        # only keep rows where start is before end
        df = df[(df['start'] <= df['end'])]

        # sort by type, start and end
        df = df.sort_values(by=['type', 'start'])

        # remove overlapping logs
        df = df.groupby('type', group_keys=False).apply(remove_overlaps, include_groups=True)

        # remove invalid values
        return df.dropna()

    async def append_log(
        self,
        guild_id: int,
        data: dict[str, any]
    ) -> None:
        """
        Adds a row of data to the end of the CSV log
        
        :param guild_id: The guild ID of the guild you want to append the logs to
        :type guild_id: int
        :param data: The data to be appended to the logs. 
        The key is the column label, the value is the data.
        :type data: dict[str, any]
        """
        log_path = self.dir.joinpath(f"{guild_id}.csv")

        # wait until logs are loaded
        while not self.loaded:
            asyncio.sleep(1)

        # append to the logs
        async with self.mutex[guild_id]:
            df = pandas.DataFrame([data])
            await asyncio.to_thread(
                lambda log_path=log_path, df=df: df.to_csv(
                    log_path, index=False,
                    encoding="utf-8", mode="a",
                    header=False
                )
            )

class TreeNextWater:
    """
    Manages the "next water" time
    """

    def __init__(
        self,
        tree_logs: TreeLogFile
    ) -> None:
        """
        Docstring for __init__
        
        :param tree_logs: the TreeLogFile instance
        :type tree_logs: TreeLogFile
        """
        self.tree_logs = tree_logs
        self.mutex = asyncio.Lock()
        self.next_water = {}
        self.loaded = False

    async def load_logs(
        self,
        guild_ids: list[int]
    ) -> None:
        """
        Loads the datetime of when each guild's tree can be watered next
        
        :param guild_ids: Guilds to update
        :type guild_ids: list[int]
        """
        async with self.mutex:
            for guild_id in guild_ids:
                # get the current time
                now = datetime.now(tz=pytz.utc)
                # fetch the most recent log
                df = await self.tree_logs.read_log(
                    guild_id=guild_id
                )
                if df is not None:
                    next_water = now
                elif df.empty:
                    next_water = now
                else:
                    next_water = df['end'].iloc[-1]
                # set default next water
                self.next_water.setdefault(guild_id, next_water)
            # signal that loading is finished
            self.loaded = True

    async def update_guild(
        self,
        guild_id: int,
        timestamp: datetime
    ) -> None:
        """
        Update the time when the tree can be watered next
        
        :param guild_id: The guild which is being updated
        :type guild_id: int
        :param timestamp: The timestamp of when it can be watered next
        :type timestamp: datetime
        """
        # wait until logs are loaded
        while not self.loaded:
            await asyncio.sleep(1)

        async with self.mutex:
            self.next_water[guild_id] = timestamp

    async def fetch_guild(
        self,
        guild_id: int
    ) -> datetime:
        """
        Fetch the time when the tree can be watered next
        
        :param guild_id: The gulid you want to fetch
        :type guild_id: int
        :return: The timestamp of when it can be watered next
        :rtype: datetime
        """
        # wait until logs are loaded
        while not self.loaded:
            await asyncio.sleep(1)

        async with self.mutex:
            return self.next_water.get(guild_id, datetime.now(tz=pytz.utc))
