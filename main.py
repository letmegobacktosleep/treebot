# import built-in packages
import logging
import asyncio
# import 3rd party packages
import discord
from discord import app_commands
from discord.ext import commands
# import utils & cogs
from utils.json import get_bot_token, BotConfigFile

# set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(filename='bot.log', level=logging.INFO)

# create the bot class
class TreeBot(commands.Bot):
    """
    Creates a Bot instance
    """

    def __init__(
            self, command_prefix, *,
            tree_cls = app_commands.CommandTree,
            allowed_contexts = app_commands.AppInstallationType(guild=True, user=False),
            allowed_installs = app_commands.AppInstallationType(guild=True, user=False),
            intents, **options
        ):

        # load the config json file
        self.config = BotConfigFile()
        asyncio.run(self.config.load_json())

        # variables from TreeLoggingCog, shared with ???
        self.datetime_next_water = {}

        # why is this so long
        super().__init__(
            command_prefix,
            tree_cls=tree_cls,
            allowed_contexts=allowed_contexts,
            allowed_installs=allowed_installs,
            intents=intents, **options
            )

    async def setup_hook(self):
        # add cogs
        await self.load_extension("cogs.treelogging")
        await self.load_extension("cogs.treenotification")
        # sync commands
        await self.tree.sync()

        return await super().setup_hook()

    async def on_ready(self):
        """
        Runs approximately when the bot has connected to the API.
        Initialises the config. 
        """
        # keep track of guilds
        guild_ids = []
        # print to console that it has started
        print(f"Logged in as user: {self.user.name}")
        for guild in self.guilds:
            guild_ids.append(guild.id)
            print(f"  {guild.id} - {guild.name}")
        # set a new config value for all the guilds
        await self.config.set_default_data(guild_ids)

    async def on_guild_join(self, guild):
        """
        Runs whenever a new guild is joined
        """
        # set a new config value for the new guild
        await self.config.set_default_data([guild.id])

# create intents
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True

# get the token and start the bot
token = get_bot_token(label="stable")
bot = TreeBot(command_prefix=None, intents=intents)
bot.run(token=token)
