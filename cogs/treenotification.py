# import built-in packages
import re
import logging
import asyncio
from typing import Optional
from datetime import datetime, timedelta
# import 3rd party packages
import pytz
import discord
from discord import app_commands
from discord.ext import commands, tasks
# import utils & cogs
from utils.constants import PATTERN_TIMESTAMP
from utils.json import BotConfigFile
from utils.config import util_modify_config
from utils.send_message import util_send_message_in_channel
from utils.treenotification_emojis import button_emojis_from_message

# set up the logger
logger = logging.getLogger(__name__)

# create a class for logging the tree watering
class TreeNotifCog(commands.Cog):
    """
    Sends messages when an action can be done to the tree it is monitoring.
    """
    def __init__(
        self,
        bot: commands.Bot,
        config: BotConfigFile
    ):
        self.bot = bot
        self.config = config
        self.message_mutex = asyncio.Lock()

        self.data_folder = "data"
        self.next_water = {}
        self.notifications = {}

    @commands.Cog.listener()
    async def on_ready(self):
        """
        Runs approximately when the bot has connected to the API.
        Initialises the time when the tree will need watering. 
        """
        # set the initial values for the "next_water"
        guild_ids = [guild.id for guild in self.bot.guilds]
        await self.set_default_guild_config(guild_ids=guild_ids)

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """
        Runs whenever a new guild is joined
        """
        # set an initial value for the next water
        await self.set_default_guild_config([guild.id])

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

        # restart the notification loop, if it is not running
        if not self.process_water_notification.is_running():
            self.process_water_notification.start()
        # restart the cleanup loop, if it is not running
        if not self.remove_notifications.is_running():
            self.remove_notifications.start()

    async def set_default_guild_config(self, guild_ids: list[int]):
        """
        Sets it to None just because.
        """
        for guild_id in guild_ids:
            # set it to None if it doesn't exist
            self.next_water.setdefault(str(guild_id), None)
            self.notifications.setdefault(str(guild_id), {"insect": None, "fruit": None, "water": None})

    async def check_tree(self, message: discord.Message):
        """
        Check if the message is a tree!
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
                    await self.process_button_notification(message=message)

    async def log_tree(self, guild_id: int, embed_text: str, edited_at: datetime):
        """
        Logs the time of:
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
            async with self.message_mutex:
                next_water = self.next_water.get(str(guild_id), datetime.now(tz=pytz.utc))
                if not (
                    timestamp <= edited_at or
                    next_water is not None and timestamp <= next_water
                ):
                    # update next_water
                    self.next_water[str(guild_id)] = timestamp

    async def process_button_notification(self, message: discord.Message):
        """
        sends a notification for insect and fruit catching
        """
        # fetch the guild id
        guild_id = message.guild.id
        if guild_id is None:
            return
        # fetch the buttons
        buttons = await button_emojis_from_message(message=message)
        # fetch the notification config
        config = await self.config.get_data(guild_id, "notification")
        # skip if channel_id is not configured
        if config["channel_id"] is None:
            return
        # check if should send an insect notification
        if config["insect"]:
            if self.tree_has_insect(buttons=buttons):
                await self.send_notification(
                    config=config,
                    guild_id=guild_id,
                    category="insect"
                )
            else:
                await self.delete_notification(
                    guild_id=guild_id,
                    category="insect"
                )
                self.notifications[str(guild_id)]["insect"] = None
        # check if should send a fruit notification
        if config["fruit"]:
            if self.tree_has_basket(buttons=buttons):
                await self.send_notification(
                    config=config,
                    guild_id=guild_id,
                    category="fruit"
                )
            else:
                await self.delete_notification(
                    guild_id=guild_id,
                    category="fruit"
                )
                self.notifications[str(guild_id)]["fruit"] = None

    @tasks.loop(seconds=1)
    async def process_water_notification(self):
        """
        sends a notification for watering
        """
        # iterate through guild IDs
        guild_ids = [guild.id for guild in self.bot.guilds]
        for guild_id in guild_ids:
            # fetch the notification config
            config = await self.config.get_data(guild_id, "notification")
            # skip if channel_id is not configured
            if config["channel_id"] is None:
                continue
            # check if should send a watering notification
            if config["water"]:
                if self.tree_needs_watering(guild_id=guild_id):
                    await self.send_notification(
                        config=config,
                        guild_id=guild_id,
                        category="water"
                    )
                else:
                    await self.delete_notification(
                        guild_id=guild_id,
                        category="water"
                    )
                    self.notifications[str(guild_id)]["water"] = None

    @tasks.loop(minutes=30)
    async def remove_notifications(self):
        """
        cleans up messages sent more than an hour ago in the tree channel
        """
        # get the current time and offset it by an hour
        cutoff = datetime.now(tz=pytz.utc) - timedelta(hours=1)
        # iterate through guild IDs
        guild_ids = [guild.id for guild in self.bot.guilds]
        for guild_id in guild_ids:
            # fetch the notification config
            config = await self.config.get_data(guild_id, "notification")
            # skip if the channel_id is not set
            channel_id = config["channel_id"]
            if channel_id is None:
                continue
            # fetch the channel
            channel = self.bot.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.bot.fetch_channel(channel_id)
                except discord.InvalidData as e:
                    logger.warning(f"Data received was invalid: {channel_id}.\n{e}")
                    return None
                except discord.NotFound as e:
                    logger.warning(f"The channel could not be found: {channel_id}.\n{e}")
                    return None
                except discord.Forbidden as e:
                    logger.warning(f"Insufficient permissions to access the channel: {channel_id}.\n{e}")
                    return None
                except discord.HTTPException as e:
                    logger.warning(f"Failed to retrieve the channel: {channel_id}.\n{e}")
                    return None
            # fetch message history
            messages = []
            async for message in channel.history(limit=200):
                if (
                    message.author == self.bot.user and
                    message.created_at < cutoff
                ):
                    messages.append(message)
            # bulk delete the messages
            if len(messages) > 0:
                await channel.delete_messages(messages, reason="Removing dead messages")
            # wait a while to prevent rate limiting
            await asyncio.sleep(10)

    async def send_notification(self, config: dict, guild_id: int, category: str):
        """
        get the notification and send the message
        """
        # only send at most one message at a time
        async with self.message_mutex:
            # skip if the message was already sent
            if self.notifications[str(guild_id)][category] is not None:
                return
            # fetch the message content and substitute pings and newlines
            content = config["message"]
            content = re.sub(r"(?i)`ping`", f"<@&{config[f"{category}_role_id"]}>", content)
            content = re.sub(r"(?i) ?`newline` ?", "\n", content)
            # figure out which part of the message to use
            index = 0
            match category:
                case "insect":
                    index = 0
                case "fruit":
                    index = 1
                case "water":
                    index = 2
            # alter the message string with the correct index
            content = re.sub(
                r"`.+?``.+?``.+?`",
                lambda match, index=index: self.substitute_string(match=match, index=index),
                content
            )
            # send the message and cache it
            self.notifications[str(guild_id)][category] = await util_send_message_in_channel(
                bot=self.bot,
                channel_id=config["channel_id"],
                content=content
            )
        # delete it if it should be temporary
        if config["temporary"]:
            await self.delete_notification(
                guild_id=guild_id,
                category=category
            )

    async def delete_notification(self, guild_id: int, category: str):
        """
        delete the cached notification
        """
        # check if the message exists
        message = self.notifications[str(guild_id)][category]
        if message is not None:
            # delete the message and remove it from cache
            await self.delete_message(message=message)

    @staticmethod
    def substitute_string(match: re.Match, index: int):
        """
        Replaces a string such as `zero``one``two` with `zero` for index 0.
        """
        match = str(match.group())
        match = match.strip("`").split("``")
        return match[index]

    @staticmethod
    def tree_has_insect(buttons):
        """
        checks whether the modal button with the bugnet exists
        """
        for button in buttons:
            if button == "bugnet":
                return True
        return False

    @staticmethod
    def tree_has_basket(buttons):
        """
        checks whether the modal button with the basket exists
        """
        for button in buttons:
            if button == "🧺":
                return True
        return False

    def tree_needs_watering(self, guild_id: int):
        """
        checks whether the current time exceeds the next watering time
        """
        next_water = self.next_water.get(str(guild_id), None)
        if next_water is not None:
            return (datetime.now(tz=pytz.utc) > next_water)
        else:
            return False

    async def delete_message(self, message: discord.Message):
        """
        helper to delete a message, logs exceptions
        """
        try:
            await message.delete()
        except discord.NotFound as e:
            logger.warning(f"The message could not be found: {message.id}.\n{e}")
            return
        except discord.Forbidden as e:
            logger.warning(f"Insufficient permissions to delete the message: {message.id}.\n{e}")
            return
        except discord.HTTPException as e:
            logger.warning(f"Failed to retrieve the message: {message.id}.\n{e}")
            return

    @app_commands.command(
        name="config_notif",
        description="message sent when an action is available"
    )
    async def cmd_set_config_notifications(
        self,
        interaction: discord.Interaction,
        channel_id: Optional[str], # too large for an int?
        temporary: Optional[bool],
        insect: Optional[bool],
        fruit: Optional[bool],
        water: Optional[bool],
        message: Optional[str],
        insect_role_id: Optional[str],
        fruit_role_id: Optional[str],
        water_role_id: Optional[str]
    ):
        """
        config category: notification
        channel_id & tree_name & outlier_duration
        """
        await util_modify_config(
            interaction=interaction,
            config_class=self.config,
            category="notification",
            config_values=[
                ("channel_id",     channel_id),
                ("temporary",      temporary),
                ("insect",         insect),
                ("fruit",          fruit),
                ("water",          water),
                ("message",        message),
                ("insect_role_id", int(insect_role_id) if insect_role_id is not None else None),
                ("fruit_role_id",  int(fruit_role_id)  if fruit_role_id  is not None else None),
                ("water_role_id",  int(water_role_id)  if water_role_id  is not None else None)
            ]
        )

# setup this file as a cog?
async def setup(bot):
    """
    cog setup
    """
    await bot.add_cog(
        TreeNotifCog(
            bot,
            bot.config
        )
    )
