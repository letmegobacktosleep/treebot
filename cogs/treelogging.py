# import built-in packages
import re
import json
import logging
import asyncio
from io import BytesIO
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta
# import 3rd party packages
import pytz
import pandas
import discord
from discord import app_commands
from discord.ext import commands, tasks
# import utils & cogs
from utils.constants import DATETIME_STRING_FORMAT, PATTERN_TIMESTAMP
from utils.json import BotConfigFile
from utils.config import util_modify_config
from utils.treelogging_graph import util_graph_summary
from utils.send_message import util_send_message_in_channel

# set up the logger
logger = logging.getLogger(__name__)

# create a class for logging the tree watering
class TreeLoggingCog(commands.Cog):
    """
    Creates a CSV log of when the tree is watered,
    and when it needs to be watered again.
    """
    def __init__(
        self,
        bot: commands.Bot,
        config: BotConfigFile
    ):
        self.bot = bot
        self.config = config
        self.mutex = asyncio.Lock()
        self.data_folder = "data"
        self.next_water = {}

    @commands.Cog.listener()
    async def on_ready(self):
        """
        Runs approximately when the bot has connected to the API.
        Initialises the time when the tree will need watering. 
        """
        # set the initial values for the "next_water"
        guild_ids = [guild.id for guild in self.bot.guilds]
        await self.set_default_next_water(guild_ids=guild_ids)

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """
        Runs whenever a new guild is joined
        """
        # set an initial value for the next water
        await self.set_default_next_water([guild.id])

    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload):
        """
        Runs whenever a message is edited.
        Grow a Tree bot always edits the message when
        - the tree is watered
        - the buttons change
        """
        try: # get the message from the payload
            message = payload.message
            # message doesn't exist
            if message is None:
                return
        except AttributeError:
            logger.warning("Payload has no message attached.")
            return
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.warning(f"Unknown error when fetching message edit.\n{e}")
            return

        # check the status of the tree, if it is a tree
        await self.check_tree(message=message)

        # restart the status message loop, if it is not running
        if not self.status_message.is_running():
            self.status_message.start()

    async def fetch_logs(
        self,
        guild_id: int,
        start: datetime,
        end: datetime
    ) -> pandas.DataFrame:
        """
        Returns the logs within the specified interval.
        """
        # get the log path
        log_path = Path(f"{self.data_folder}/{guild_id}.csv")
        if not log_path.exists():
            return None

        # read the csv log
        async with self.mutex:
            df = await asyncio.to_thread(
                lambda log_path=log_path: pandas.read_csv(
                    filepath_or_buffer=log_path,
                    parse_dates=['wet', 'dry'],
                    date_format=DATETIME_STRING_FORMAT
                )
            )

        # set timezone as UTC
        df[['wet', 'dry']] = df[['wet', 'dry']].apply(
            lambda col: col.dt.tz_localize(pytz.utc)
        )

        # only keep rows where wet is before dry
        df = df[(df['wet'] <= df['dry'])]

        # filter within the specified interval
        df = df[(df['dry'] >= start) & (df['wet'] <= end)]

        # remove invalid values
        return df.dropna()

    async def set_default_next_water(
        self,
        guild_ids: list[int]
    ) -> None:
        """
        Reads the latest "next_water" time from the CSV logs,
        Or sets it as the current time if the CSV logs do not exist.
        """
        for guild_id in guild_ids:
            # figure out the log path
            log_path = Path(f"{self.data_folder}/{guild_id}.csv")
            # get the current time
            now = datetime.now(tz=pytz.utc)
            # fetch the most recent log
            if log_path.exists():
                cutoff = now - timedelta(hours=3)
                df = await self.fetch_logs(
                    guild_id=guild_id,
                    start=cutoff,
                    end=now
                )
                if df.empty:
                    next_water = now
                else:
                    next_water = df['dry'].iloc[-1]
            # create a blank log
            else:
                async with self.mutex:
                    df = pandas.DataFrame(
                        columns=['wet', 'dry']
                    )
                    await asyncio.to_thread(
                        lambda log_path=log_path, df=df: df.to_csv(
                            log_path, index=False,
                            encoding="utf-8"
                        )
                    )
                next_water = now
            # set it to the "next water" time if it doesn't exist
            self.next_water.setdefault(str(guild_id), next_water)

    async def check_tree(
        self,
        message: discord.Message
    ) -> None:
        """
        Checks whether the message is from "Grow a Tree",
        Calls log_tree and check_goal if it is. 
        """
        try: # get the edited_at timestamp from the message
            edited_at = message.edited_at
            # edited at doesn't exist somehow
            if edited_at is None:
                return
        except AttributeError:
            logger.warning("Message has no edited_at attribute.")
            return
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.warning(f"Unknown error when fetching edited_at.\n{e}")
            return

        # fetch the general config
        config = await self.config.get_data(message.guild.id, "general")
        # skip if the config is not set up correctly
        if config["channel_id"] is None or config["tree_name"] is None:
            return
        # in the correct channel and with embeds
        if message.channel.id == config["channel_id"] and message.embeds:
            for embed in message.embeds:
                if (
                    hasattr(embed, "title") and 
                    embed.title is not None and
                    config["tree_name"] in embed.title and
                    hasattr(embed, "description") and
                    embed.description is not None and
                    "your tree is" in embed.description.lower()
                ):
                    embed_text = f"{embed.description}\n{embed.footer.text}"
                    await self.log_tree(guild_id=message.guild.id, embed_text=embed_text, edited_at=edited_at)
                    await self.check_goal(guild_id=message.guild.id, embed_text=embed_text)

    async def log_tree(
        self,
        guild_id: int,
        embed_text: str,
        edited_at: datetime
    ) -> None:
        """
        Logs the time of:
        - when the message was last edited
        - when the tree can be watered next
        """
        # usually only the message displaying the tree will contain "ready to be watered"
        if not "ready to be watered" in embed_text.lower():
            # look for the timestamp of when it can be watered next
            timestamp = PATTERN_TIMESTAMP.search(embed_text)
            # timestamp was not found
            if timestamp is None:
                logger.info(f"Could not find timestamp <t:12345678:R> in embed text {embed_text.replace('\n', '')}")
                return
            # timestamp was found
            timestamp = int(timestamp.group())
            timestamp = datetime.fromtimestamp(timestamp=timestamp, tz=pytz.utc)
            # check if it is before edited_at or next_water
            async with self.mutex:
                if (
                    timestamp <= edited_at or
                    timestamp <= self.next_water.get(str(guild_id), datetime.now(tz=pytz.utc))
                ):
                    return
                # append edited_at and timestamp to the log
                log_path = Path(f"{self.data_folder}/{guild_id}.csv")
                df = pandas.DataFrame([{
                    'wet': edited_at.strftime(DATETIME_STRING_FORMAT),
                    'dry': timestamp.strftime(DATETIME_STRING_FORMAT)
                }])
                await asyncio.to_thread(
                    lambda df=df: df.to_csv(
                        log_path, index=False,
                        encoding="utf-8", mode="a",
                        header=False
                    )
                )
                # update next_water
                self.next_water[str(guild_id)] = timestamp

    async def check_goal(
        self,
        guild_id: int,
        embed_text: str
    ) -> None:
        """
        Checks whether the goal has been reached,
        and sends a notification if it has.
        """
        # fetch the goal config
        config = await self.config.get_data(guild_id, "tree_goal")
        # ignore if no channel_id has been set
        if config["channel_id"] is None:
            return
        # check whether the goal has been reached
        if config["reached"]:
            return
        # look for the pattern in the embed text
        value = re.search(config["pattern"], embed_text)
        if value is None:
            logger.info(f"Could not find pattern: {config["pattern"]} in embed text {embed_text}")
            return
        else:
            value = float(value.group())
        # check whether it has passed the goal
        if (
            (config["greater_than"] and value >= config["goal"]) or
            (not config["greater_than"] and value <= config["goal"])
        ):
            # create the notification message
            content = config["message"]
            def substitute_string(match):
                return f"<@{re.search(r"&?[0-9]+", match.group()).group()}>"
            content = re.sub(r"`@/[0-9]+`", substitute_string, content)
            content = re.sub(r"(?i)(?<=`)goal(?=`)", f"{config["goal"]}", content)
            content = re.sub(r"(?i) ?`newline` ?", "\n", content)
            # send the notification message
            message = await util_send_message_in_channel(
                bot=self.bot,
                channel_id=config["channel_id"],
                content=content
            )
            if message is not None:
                # update "reached" to True
                config["reached"] = True
                # save the config
                config = await self.config.set_data(guild_id, "tree_goal", config)

    async def calc_up_down(
        self,
        guild_id: int,
        hours: int
    ) -> tuple:
        """
        Returns a tuple containing (uptime, downtime) within the past x hours.
        """
        # get current time and find the cutoff
        now = datetime.now(tz=pytz.utc)
        cutoff = now - timedelta(hours=hours)

        # fetch the logs
        df = await self.fetch_logs(
            guild_id=guild_id,
            start=cutoff,
            end=now
        )

        # clamp values
        df.loc[df['wet'] <= cutoff, 'wet'] = cutoff
        df.loc[df['dry'] >= now,    'dry'] = now

        # calculate uptime and downtime
        df['uptime']   = df['dry'] - df['wet']
        df['downtime'] = df['wet'] - df['dry'].shift(1)
        df['uptime']    = df['uptime'].dt.total_seconds()
        df['downtime']  = df['downtime'].dt.total_seconds()

        # remove outliers
        config = await self.config.get_data(guild_id, "general")
        max_duration = config["outlier_duration"]
        df = df[(df['downtime'] < max_duration) & (df['uptime'] < max_duration)]

        # get the sum of uptime and downtime
        uptime   = df['uptime'].sum()
        downtime = df['downtime'].sum()

        # account for currently downtime, if the dataframe exists
        if not df.empty:
            last_dry = df['dry'].iloc[-1]
            if last_dry < now:
                downtime += (now - last_dry).total_seconds()

        # return as a tuple
        return (uptime, downtime)

    @tasks.loop(minutes=1)
    async def status_message(self):
        """
        Sends a status message summarising the uptime and downtime.
        """
        # fetch the current time with hour precision
        dt = datetime.now(tz=pytz.utc)
        # iterate through guild IDs
        guild_ids = [guild.id for guild in self.bot.guilds]
        for guild_id in guild_ids:
            # fetch the status message config
            config = await self.config.get_data(guild_id, "status_message")
            # check whether a message should be sent
            if (
                config["channel_id"] is not None and
                config["valid_days"][0] <= dt.weekday() and
                config["valid_days"][1] >= dt.weekday()
            ):
                for i, hour in enumerate(config["valid_hours"]):
                    next_message = config["next_message"][i]
                    next_message = next_message.strptime(DATETIME_STRING_FORMAT)
                    next_message = next_message.replace(tzinfo=pytz.utc)
                    if dt > next_message:
                        # find the uptime and downtime
                        h_uptime, h_downtime = self.calc_up_down(
                            guild_id=guild_id,
                            hours=config["total_hours"]
                        )
                        y_uptime, y_downtime = self.calc_up_down(
                            guild_id=guild_id,
                            hours=24*365
                        )
                        # try to send the message
                        message = await util_send_message_in_channel(
                            bot=self.bot,
                            channel_id=config["channel_id"],
                            content=(
                                f"### Past {config["total_hours"]} Hours:\n"
                                f"`uptime:` `{100 * h_uptime / (h_uptime + h_downtime + 0.00001):7.4f}%`   "
                                f"`wet:` `{h_uptime:6.0f}`   `dry:` `{h_downtime:6.0f}`\n"
                                f"### Past Year:\n"
                                f"`uptime:` `{100 * y_uptime / (y_uptime + y_downtime + 0.00001):7.4f}%`   "
                                f"`wet:` `{y_uptime:6.0f}`   `dry:` `{y_downtime:6.0f}`\n"
                            )
                        )
                        if message is not None:
                            # update the time for the next message
                            next_message = dt.replace(hour=hour, minute=0, second=0, microsecond=0)
                            next_message = next_message + timedelta(days=1)
                            config["next_message"][i] = next_message.strftime(DATETIME_STRING_FORMAT)
                            await self.config.get_data(guild_id, "status_message", config)

    @app_commands.command(
        name="config_general",
        description="where the tree is located"
    )
    async def cmd_set_config_logs(
        self,
        interaction: discord.Interaction,
        channel_id: Optional[str], # too large for an int?
        tree_name: Optional[str],
        output_timezone: Optional[str],
        outlier_duration: Optional[int]
    ) -> None:
        """
        config category: general
        channel_id & tree_name & outlier_duration
        """
        await util_modify_config(
            interaction=interaction,
            config_class=self.config,
            category="general",
            config_values=[
                ("channel_id",       channel_id),
                ("tree_name",        tree_name),
                ("timezone",         output_timezone),
                ("outlier_duration", outlier_duration)
            ]
        )

    @app_commands.command(
        name="config_status",
        description="where the status messages are sent (valid_xxx are comma separated integers)"
    )
    async def cmd_set_config_status(
        self,
        interaction: discord.Interaction,
        channel_id: Optional[str], # too large for an int?
        total_hours: Optional[int],
        valid_days: Optional[str],
        valid_hours: Optional[str]
    ) -> None:
        """
        config category: status_message
        channel_id, total_hours, valid_days, valid_hours
        """
        await util_modify_config(
            interaction=interaction,
            config_class=self.config,
            category="status_message",
            config_values=[
                ("channel_id",  channel_id),
                ("total_hours", total_hours),
                ("valid_days",  [int(i.strip()) for i in valid_days.split(",")]  if valid_days  is not None else None),
                ("valid_hours", [int(i.strip()) for i in valid_hours.split(",")] if valid_hours is not None else None)
            ]
        )

    @app_commands.command(
        name="config_goal",
        description="conditions for reaching the goal"
    )
    async def cmd_set_config_goal(
        self,
        interaction: discord.Interaction,
        channel_id: Optional[str], # too large for an int?
        goal: Optional[int],
        greater_than: Optional[bool],
        pattern: Optional[str],
        message: Optional[str],
        reached: Optional[bool]
    ) -> None:
        """
        config category: tree_goal
        channel_id, goal, greater_than, pattern, message, reached
        """
        if goal is not None:
            reached = False
        await util_modify_config(
            interaction=interaction,
            config_class=self.config,
            category="tree_goal",
            config_values=[
                ("channel_id",   channel_id),
                ("goal",         goal),
                ("greater_than", greater_than),
                ("pattern",      pattern),
                ("message",      message),
                ("reached",      reached)
            ]
        )

    @app_commands.command(
        name="watering_logs",
        description="fetches a portion of the logs"
    )
    async def cmd_fetch_logs(
        self,
        interaction: discord.Interaction,
        hours: int,
        offset: int = 0
    ) -> None:
        """
        Send the logs as a CSV attachment
        """
        # ensure that the guild_id exists
        if not interaction.guild_id:
            interaction.response.send_message(
                content="Command can only be used in guilds.",
                ephemeral=True
            )
            return
        # defer the interaction so it doesn't time out
        await interaction.response.defer(ephemeral=True, thinking=True)
        # set the guild id
        guild_id = interaction.guild_id
        # get the time period
        now = datetime.now(tz=pytz.utc)
        start = now - timedelta(hours=offset)
        cutoff = start - timedelta(hours=hours)
        # fetch the logs within the time period
        df = await self.fetch_logs(
            guild_id=guild_id,
            start=cutoff,
            end=now
        )
        # create a BytesIO object to store the logs in memory
        buffer = BytesIO()
        await asyncio.to_thread(
            lambda file=buffer, df=df: df.to_csv(
                file, index=False,
                encoding="utf-8"
            )
        )
        # convert into a discord file
        buffer.seek(0)
        date_format = "%Y-%m-%d_%H:%M:%S"
        file = discord.File(
            fp=buffer,
            filename=(
                f"{guild_id}_"
                f"{start.strftime(date_format)}_to_"
                f"{start.strftime(date_format)}.csv"
            )
        )
        # send the file
        await interaction.followup.send(
            content="MESSAGE LOGS CHANGE THIS MESSAGE",
            file=file
        )

    @app_commands.command(
        name="watering_graph",
        description="generates a summary graph"
    )
    async def cmd_calc_graph(
        self,
        interaction: discord.Interaction,
        hours: int,
        offset: int = 0
    ) -> None:
        """
        Send a graph generated from the logs
        """
        # ensure that the guild_id exists
        if not interaction.guild_id:
            interaction.response.send_message(
                content="Command can only be used in guilds.",
                ephemeral=True
            )
            return
        # defer the interaction so it doesn't time out
        await interaction.response.defer(ephemeral=True, thinking=True)
        # set the guild id
        guild_id = interaction.guild_id
        # get the time period
        now = datetime.now(tz=pytz.utc)
        start = now - timedelta(hours=offset)
        cutoff = start - timedelta(hours=hours)
        # fetch the logs within the time period
        df = await self.fetch_logs(
            guild_id=guild_id,
            start=cutoff,
            end=now
        )
        # generate the graph
        config = await self.config.get_data(guild_id, "general")
        try:
            output_timezone = pytz.timezone(config["timezone"])
        except pytz.exceptions.UnknownTimeZoneError:
            await interaction.followup.send(
                content=(
                    "Invalid timezone. Please change the timezone in general config\n"
                    "Example configuration: `/config_general timezone:UTC` `/config_general timezone:Australia/Sydney`"
                )
            )
        buffer = await util_graph_summary(
            df=df,
            max_duration=config["outlier_duration"],
            output_timezone=output_timezone
        )
        # convert into a discord file
        buffer.seek(0)
        date_format = "%Y-%m-%d_%H:%M:%S"
        file = discord.File(
            fp=buffer,
            filename=(
                f"{guild_id}_"
                f"{start.strftime(date_format)}_to_"
                f"{start.strftime(date_format)}.png"
            )
        )
        # send the file
        await interaction.followup.send(
            content="SUMMARY GRAPH CHANGE THIS MESSAGE",
            file=file
        )

# setup this file as a cog?
async def setup(bot):
    """
    cog setup
    """
    await bot.add_cog(
        TreeLoggingCog(
            bot,
            bot.config
        )
    )
