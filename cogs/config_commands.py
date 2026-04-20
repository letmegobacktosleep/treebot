# import built-in packages
import logging
import re
# import 3rd party packages
import asyncio
import discord
from discord import app_commands
from discord.ext import commands
# import utils & cogs
from utils.json import BotConfigFile
from utils.config import util_modify_config
from utils.send_message import util_delete_message

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
        channel: discord.TextChannel | None,
        tree_name: str               | None,
        output_timezone: str         | None,
        outlier_duration: int        | None
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
                ("channel_id",       channel.id if channel is not None else None),
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
        channel: discord.TextChannel | None,
        total_hours: int             | None,
        valid_days: str              | None,
        valid_hours: str             | None
    ) -> None:
        """
        config category: status_message
        channel_id, total_hours, valid_days, valid_hours
        """
        next_message = None
        # Convert valid_days to a list of ints
        if valid_days is not None:
            valid_days_int = [int(i.strip()) for i in valid_days.split(",")]
            next_message = []
        else:
            valid_days_int = None
        # Convert valid_hours to a list of ints
        if valid_hours is not None:
            valid_hours_int = [int(i.strip()) for i in valid_hours.split(",")]
            next_message = []
        else:
            valid_hours_int = None

        await util_modify_config(
            interaction=interaction,
            config_class=self.config,
            category="status_message",
            config_values=[
                ("channel_id",  channel.id if channel is not None else None),
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
        channel: discord.TextChannel | None,
        goal: int                    | None,
        greater_than: bool           | None,
        pattern: str                 | None,
        message: str                 | None,
        reached: bool                | None
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
                ("channel_id",   channel.id if channel is not None else None),
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
    @app_commands.choices(persistence=[
        app_commands.Choice(name="delete_immediately", value="delete_immediately"),
        app_commands.Choice(name="delete_after_event", value="delete_after_event"),
        app_commands.Choice(name="delete_after_expiry", value="delete_after_expiry"),
        app_commands.Choice(name="never_delete", value="never_delete"),
    ])
    async def cmd_set_config_notifications(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel          | None,
        persistence: app_commands.Choice[str] | None,
        max_notif_age: int                    | None,
        water_notif_delay: int                | None,
        insect: bool                          | None,
        fruit: bool                           | None,
        water: bool                           | None,
        early_water: bool                     | None,
        message: str                          | None,
        insect_role: discord.Role             | None,
        fruit_role: discord.Role              | None,
        water_role: discord.Role              | None,
        early_water_role: discord.Role        | None,
    ):
        """
        config category: notification
        channel_id & tree_name & outlier_duration
        """

        persistence_str = None
        if persistence is not None:
            persistence_str = persistence.value

        await util_modify_config(
            interaction=interaction,
            config_class=self.config,
            category="notification",
            config_values=[
                ("channel_id",          channel.id if channel is not None else None),
                ("persistence",         persistence_str),
                ("max_notif_age",       max_notif_age),
                ("water_notif_delay",   water_notif_delay),
                ("insect",              insect),
                ("fruit",               fruit),
                ("water",               water),
                ("early_water",         early_water),
                ("message",             message),
                ("insect_role_id",      insect_role.id      if insect_role is not None else None),
                ("fruit_role_id",       fruit_role.id       if fruit_role is not None else None),
                ("water_role_id",       water_role.id       if water_role is not None else None),
                ("early_water_role_id", early_water_role.id if early_water_role is not None else None)
            ]
        )

    @app_commands.command(
        name="config_notif_test",
        description="test whether the bot can send messages"
    )
    @app_commands.choices(category=[
        app_commands.Choice(name="insect", value="insect"),
        app_commands.Choice(name="fruit", value="fruit"),
        app_commands.Choice(name="water", value="water"),
    ])
    async def cmd_test_notifications(
        self,
        interaction: discord.Interaction,
        category: app_commands.Choice[str]
    ):
        """
        Test whether the notification works
        """
        if not interaction.guild:
            await interaction.response.send_message(
                content="This command must be used in a guild",
                ephemeral=True
            )
            return

        config = await self.config.get_data(
            interaction.guild.id,
            "notification"
        )

        # fetch the message content and substitute pings and newlines
        content = config["message"]
        content = re.sub(r"(?i)`ping`", "<role ping>", content)
        content = re.sub(r"(?i) ?`newline` ?", "\n", content)
        # figure out which part of the message to use
        index_map = {
            "insect": 0,
            "fruit": 1,
            "water": 2
        }
        index = index_map.get(category.value, 0)
        # function for string substitution
        def substitute_string(match: re.Match, index: int) -> str:
            """
            Replaces a string such as `zero``one``two` with `zero` for index 0.
            """
            match = str(match.group())
            match = match.strip("`").split("``")
            return match[index]
        # alter the message string with the correct index
        content = re.sub(
            r"`.+?``.+?``.+?`",
            lambda match, index=index: substitute_string(match=match, index=index),
            content
        )

        # fetch the channel
        channel_id = config["channel_id"]
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except discord.InvalidData as e:
                await interaction.response.send_message(
                    content=f"Data received was invalid: ({channel_id}).```\n{e}```",
                    ephemeral=True
                )
                return
            except discord.NotFound as e:
                await interaction.response.send_message(
                    content=f"The channel could not be found: ({channel_id}).```\n{e}```",
                    ephemeral=True
                )
                return

            except discord.Forbidden as e:
                await interaction.response.send_message(
                    content=f"Insufficient permissions to access the channel: ({channel_id}).```\n{e}```",
                    ephemeral=True
                )
                return

            except discord.HTTPException as e:
                await interaction.response.send_message(
                    content=f"Failed to retrieve the channel: ({channel_id}).```\n{e}```",
                    ephemeral=True
                )
                return

        # skip if no permission to send messages
        permissions = channel.permissions_for(channel.guild.me)
        if not permissions.send_messages:
            await interaction.response.send_message(
                content=f"No permission to send messages in channel <#{channel_id}> ({channel_id})",
                ephemeral=True
            )
            return

        # send the message
        try:
            message = await channel.send(content=content, files=None)
        except discord.NotFound as e:
            await interaction.response.send_message(
                content=f"The channel could not be found: <#{channel_id}> ({channel_id}).```\n{e}```",
                ephemeral=True
            )
            return
        except discord.Forbidden as e:
            await interaction.response.send_message(
                content=f"Insufficient permissions to send messages in channel: <#{channel_id}> ({channel_id}).```\n{e}```",
                ephemeral=True
            )
            return
        except ValueError as e:
            await interaction.response.send_message(
                content=f"You specified both file and files, or you specified both embed and embeds, or the reference object is not a Message, MessageReference or PartialMessage: {channel_id}.```\n{e}```",
                ephemeral=True
            )
            return
        except discord.HTTPException as e:
            await interaction.response.send_message(
                content=f"Failed to retrieve the channel: {channel_id}.```\n{e}```",
                ephemeral=True
            )
            return

        await interaction.response.send_message(
            content=f"Successfully sent message in <#{channel.id}> ({channel_id}) : {message.jump_url}.",
            ephemeral=True
        )

        await asyncio.sleep(10)
        await util_delete_message(message=message)

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
