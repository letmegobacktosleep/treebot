# import built-in packages
import re
import logging
import asyncio
from io import BytesIO
from datetime import datetime, timedelta
# import 3rd party packages
import pytz
import discord
from discord import app_commands
from discord.ext import commands, tasks
# import utils & cogs
from utils.constants import DATETIME_STRING_FORMAT, PATTERN_TIMESTAMP
from utils.tree_logs import TreeLogFile, TreeNextWater
from utils.json import BotConfigFile
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
        config: BotConfigFile,
        tree_logs: TreeLogFile,
        next_water: TreeNextWater
    ):
        self.bot = bot
        self.config = config
        self.tree_logs = tree_logs
        self.next_water = next_water
        self.data_folder = "data"

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
                logger.info("Could not find timestamp <t:12345678:R> in embed text " + embed_text.replace("\n", ""))
                return
            # timestamp was found
            timestamp = int(timestamp.group())
            timestamp = datetime.fromtimestamp(timestamp=timestamp, tz=pytz.utc)
            # fetch the next water time
            next_water, water_duration = await self.next_water.fetch_guild(guild_id=guild_id)
            # skip logging if it is before edited_at or next_water
            if (
                timestamp <= edited_at or
                timestamp <= next_water
            ):
                return
            # offset if the time delta is less than the last recorded duration
            if (timestamp - edited_at) < water_duration:
                edited_at = timestamp - water_duration
            # append edited_at and timestamp to the log
            await self.tree_logs.append_log(
                guild_id=guild_id,
                data = {
                    'start': edited_at.strftime(DATETIME_STRING_FORMAT),
                    'end':   timestamp.strftime(DATETIME_STRING_FORMAT),
                    'type': "water"
                }
            )
            # update next_water
            await self.next_water.update_guild(
                guild_id=guild_id,
                timestamp=timestamp,
                duration=timestamp - edited_at
            )

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
            # logger.info(f"Could not find pattern: {config['pattern']} in embed text {embed_text}")
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
                user_or_role_id = re.search(r"&?[0-9]+", match.group()).group()
                return f"<@{user_or_role_id}>"
            content = re.sub(r"`@/[0-9]+`", substitute_string, content)
            content = re.sub(r"(?i)(?<=`)goal(?=`)", f"{config['goal']}", content)
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
        df = await self.tree_logs.read_log(
            guild_id=guild_id,
            start=cutoff,
            end=now
        )

        # clamp values
        df.loc[df['start'] <= cutoff, 'start'] = cutoff
        df.loc[df['end']   >= now,    'end']   = now

        # calculate uptime and downtime
        df['uptime']   = df['end'] - df['start']
        df['downtime'] = df['start'] - df['end'].shift(1)
        df['uptime']    = df['uptime'].dt.total_seconds()
        df['downtime']  = df['downtime'].dt.total_seconds()

        # remove outliers
        config = await self.config.get_data(guild_id, "general")
        max_duration = config["outlier_duration"]
        df = df[(df['downtime'] < max_duration) & (df['uptime'] < max_duration)]

        # get the sum of uptime and downtime
        uptime   = df['uptime'].sum()
        downtime = df['downtime'].sum()

        # no datapoints in the dataframe
        if df.empty:
            return (0, hours * 60 * 60)
        else: # last datapoint ends before current time
            last_dry = df['end'].iloc[-1]
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
                dt.weekday() in config["valid_days"]
            ):
                for i, hour in enumerate(config["valid_hours"]):
                    # doesn't exist yet
                    if i >= len(config["next_message"]):
                        # set the hour
                        next_message = dt.replace(hour=hour, minute=0, second=0, microsecond=0)
                        # shift until the next valid day
                        while next_message.weekday() not in config["valid_days"]:
                            next_message = next_message + timedelta(days=1)
                        # add to next_message
                        config["next_message"].append(next_message.strftime(DATETIME_STRING_FORMAT))
                        await self.config.set_data(guild_id, "status_message", config)
                    # remove unused next_message
                    if len(config["next_message"]) > len(config["valid_hours"]):
                        config["next_message"] = config["next_message"][:len(config["valid_hours"])]
                        await self.config.set_data(guild_id, "status_message", config)
                    # check the config
                    next_message = config["next_message"][i]
                    next_message = datetime.strptime(next_message, DATETIME_STRING_FORMAT)
                    next_message = next_message.replace(tzinfo=pytz.utc)
                    if dt > next_message:
                        # find the uptime and downtime
                        h_uptime, h_downtime = await self.calc_up_down(
                            guild_id=guild_id,
                            hours=config["total_hours"]
                        )
                        y_uptime, y_downtime = await self.calc_up_down(
                            guild_id=guild_id,
                            hours=24*365
                        )
                        # calculate the number of days and hours
                        days = config['total_hours'] // 24
                        hours = config['total_hours'] % 24
                        time_delta_str = f"{days} days"
                        if hours:
                            time_delta_str += f", {hours} hours"
                        # try to send the message
                        message = await util_send_message_in_channel(
                            bot=self.bot,
                            channel_id=config["channel_id"],
                            content=(
                                f"### Past {time_delta_str}:\n"
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
                            await self.config.set_data(guild_id, "status_message", config)
                            # only send one message per server every time the loop runs
                            break

    @app_commands.command(
        name="watering_logs",
        description="fetches a portion of the logs"
    )
    async def cmd_fetch_logs(
        self,
        interaction: discord.Interaction,
        days: int,
        hours: int = 0,
        offset_days: int = 0,
        offset_hours: int = 0
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
        end = now - timedelta(days=offset_days, hours=offset_hours)
        start = end - timedelta(days=days, hours=hours)
        # fetch the logs within the time period
        df = await self.tree_logs.read_log(
            guild_id=guild_id,
            start=start,
            end=end,
            filter_logs=None
        )
        # error if there are no logs
        if df is None or df.empty:
            await interaction.followup.send(
                content=(
                    "The requested logs contain no data.\n"
                    "Please try the command again with a longer timeframe."
                )
            )
            return
        # fetch the output timezone
        config = await self.config.get_data(guild_id, "general")
        try:
            output_timezone = pytz.timezone(config["timezone"])
        except pytz.exceptions.UnknownTimeZoneError:
            await interaction.followup.send(
                content=(
                    "Invalid timezone. Please change the timezone in general config\n"
                    "Example configuration: `/config_general timezone:UTC` "
                    "`/config_general timezone:Australia/Sydney`"
                )
            )
            return
        # convert timezones
        df[['start', 'end']] = df[['start', 'end']].apply(
            lambda col: col.dt.tz_convert(output_timezone)
        )
        # convert datetime into string
        df[['start', 'end']] = df[['start', 'end']].apply(
            lambda col: col.dt.strftime(DATETIME_STRING_FORMAT)
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
                f"{start.astimezone(tz=output_timezone).strftime(date_format)}_to_"
                f"{end.astimezone(tz=output_timezone).strftime(date_format)}.csv"
            )
        )
        # send the file
        await interaction.followup.send(
            content=(
                f"`Type:     ` Watering Logs\n"
                f"`Timezone: ` {config['timezone']}\n"
                f"`Start:    ` <t:{start.timestamp():.0f}:f>\n"
                f"`End:      ` <t:{end.timestamp():.0f}:f>"
            ),
            file=file
        )

    @app_commands.command(
        name="watering_graph",
        description="generates a summary graph"
    )
    async def cmd_calc_graph(
        self,
        interaction: discord.Interaction,
        days: int,
        hours: int = 0,
        offset_days: int = 0,
        offset_hours: int = 0
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
        end = now - timedelta(days=offset_days, hours=offset_hours)
        start = end - timedelta(days=days, hours=hours)
        # fetch the logs within the time period
        df = await self.tree_logs.read_log(
            guild_id=guild_id,
            start=start,
            end=end
        )
        # error if there are no logs
        if df.empty:
            await interaction.followup.send(
                content=(
                    "The requested logs contain no data.\n"
                    "Please try the command again with a longer timeframe."
                )
            )
            return
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
                f"{start.astimezone(tz=output_timezone).strftime(date_format)}_to_"
                f"{end.astimezone(tz=output_timezone).strftime(date_format)}.png"
            )
        )
        # send the file
        await interaction.followup.send(
            content=(
                f"`Type:     ` Summary Graph\n"
                f"`Timezone: ` {config['timezone']}\n"
                f"`Start:    ` <t:{start.timestamp():.0f}:f>\n"
                f"`End:      ` <t:{end.timestamp():.0f}:f>"
            ),
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
            bot.config,
            bot.tree_logs,
            bot.next_water
        )
    )
