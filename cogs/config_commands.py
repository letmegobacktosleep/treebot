# import built-in packages
import logging
from typing import Optional
# import 3rd party packages
import discord
from discord import app_commands
from discord.ext import commands
# import utils & cogs
from utils.json import BotConfigFile
from utils.config import util_modify_config

# set up the logger
logger = logging.getLogger(__name__)

# create a class for logging the tree watering
class ConfigCog(commands.Cog):
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
        # Convert valid_days to a list of ints
        if valid_days is not None:
            valid_days_int = [int(i.strip()) for i in valid_days.split(",")]
        else:
            valid_days_int = None
        # Convert valid_hours to a list of ints
        if valid_hours is not None:
            valid_hours_int = [int(i.strip()) for i in valid_hours.split(",")]
            next_message = []
        else:
            valid_hours_int = None
            next_message = None

        await util_modify_config(
            interaction=interaction,
            config_class=self.config,
            category="status_message",
            config_values=[
                ("channel_id",  channel_id),
                ("total_hours", total_hours),
                ("valid_days",  valid_days_int),
                ("valid_hours", valid_hours_int),
                ("next_message", next_message)
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
        ConfigCog(
            bot,
            bot.config
        )
    )
