# import built-in packages
import os
import json
import logging
from pathlib import Path
from datetime import datetime
import asyncio
# import 3rd party packages
import pytz
import aiofiles
# import utils & cogs
from utils.constants import DATETIME_STRING_FORMAT

# set up the logger
logger = logging.getLogger(__name__)

def get_bot_token(path: str = "token.json", label: str = "stable"):
    """
    Retrieves a bot token from a JSON file.
    Index 0 is for deployment, index 1 is for testing.
    """
    if not os.path.exists(path=path):
        with open(path, mode="w", encoding="utf-8") as f:
            json.dump({"stable": {"token": "INSERT_BOT_TOKEN_HERE"}}, f)

    with open(path, mode="r", encoding="utf-8") as f:
        data = json.load(f)

    token = data[label]["token"]
    if token == "INSERT_BOT_TOKEN_HERE":
        raise FileExistsError("Please insert your bot token in \'token.json\'")

    return token

class BotConfigFile:
    """
    Manages the JSON file which stores the config.
    Remember to call .load_json() to load from the file.
    """

    def __init__(self, path: str = "data/config.json"):
        self.path = Path(path)
        self.data = {}
        self.mutex = asyncio.Lock()
        self.loaded = False

    async def load_json(self):
        """
        Loads the JSON file from the disk
        """
        # if the json file exists
        if self.path.exists():
            # open the json file for reading
            async with aiofiles.open(self.path, "r", encoding="utf-8") as f:
                # read the json file contents as a string
                contents = await f.read()
            # load json into the dictionary
            async with self.mutex:
                self.data = json.loads(contents)
        # the json file doesn't exist
        else:
            await self.save_json()
        # set the "loaded" flag
        self.loaded = True

    async def save_json(self):
        """
        Saves the current data to the JSON file
        """
        # convert the data to a json string
        async with self.mutex:
            contents = json.dumps(self.data, indent=4)
        # open json file for writing
        async with aiofiles.open(self.path, "w", encoding="utf-8") as f:
            # write the current data to the json
            await f.write(contents)

    async def get_data(self, guild_id: int, key: str) -> dict:
        """
        Gets the data with a specified key for a specified guild
        """
        async with self.mutex:
            try:
                data = self.data.get(str(guild_id), {}).get(key)
            except KeyError as e:
                logger.warning(f"JSON file does not have key: [{guild_id}][{key}]\n{e}")
                return None
            except Exception as e: # pylint: disable=broad-exception-caught
                logger.warning(f"Unknown Exception: \n{e}")
                return None
            if data is None:
                logger.warning(f"Data for guild {guild_id} with key {key} is None")
                return None
            else:
                return data.copy()

    async def set_data(self, guild_id: int, key: str, data: dict) -> bool:
        """
        Sets the data with a specified key for a specified guild
        """
        async with self.mutex:
            try:
                self.data.setdefault(str(guild_id), {})
                self.data[str(guild_id)][key] = data
            except KeyError as e:
                logger.warning(f"Unknown KeyError: [{guild_id}][{key}]\n{e}")
                return False
            except Exception as e: # pylint: disable=broad-exception-caught
                logger.warning(f"Unknown Exception: \n{e}")
                return False
        # save the new data to the json file
        await self.save_json()
        return True

    async def set_default_data(self, guild_ids: list[int]):
        """
        Sets default data for guilds not in the JSON file
        """
        # fetch the current time with hour precision
        dt = datetime.now(tz=pytz.utc)
        dt = dt.replace(minute=0, second=0, microsecond=0)
        # acquire mutex lock
        async with self.mutex:
            # iterate through list of guild ids
            for guild_id in guild_ids:
                guild_id = str(guild_id)
                self.data.setdefault(
                    guild_id,
                    {
                        "general": {
                            "tree_name": None, # the name of the tree # string, ignore if None
                            "channel_id": None, # which channel the tree is in # int, ignore if None
                            "timezone": "UTC",
                            "outlier_duration": 60 * 60 * 2 # maximum number of seconds before it is counted as an outlier
                        },
                        "status_message": {
                            "channel_id": None, # integer, ignore if None
                            "total_hours": 24 * 7,
                            "valid_days": [6], # valid days (day 6 = Sunday)
                            "valid_hours": [11], # valid hours (11am UTC ~= 9pm AEST)
                            "next_message": [dt.replace(hour=11).strftime(DATETIME_STRING_FORMAT)] # when the next message should be sent
                        },
                        "tree_goal": {
                            "channel_id": None, # integer, ignore if None
                            "reached": True, # whether the goal has been reached
                            "goal": 0, # default to zero - should never be reached
                            "greater_than": False, # whether it should check if the value is greater than
                            "pattern": "(?<=the #)[0-9]*(?= tallest)", # the regex pattern to find the float in the string
                            "message": "`@/` `newline` Tree has reached rank #`goal`!" # the message to be sent when the goal is reached
                        },
                        "notification": {
                            "channel_id": None, # integer, ignore if None
                            "insect": False, # bool, whether to notify for an insect
                            "fruit": False, # bool, whether to notify for an insect
                            "water": False, # bool, whether to notify for an insect
                            "temporary": True, # bool, whether to delete the message immediately after sending it
                            "message": "`ping` `Catch the insect!``Collect the fruit!``Water the tree!`",
                            "insect_role_id": "",
                            "fruit_role_id": "",
                            "water_role_id": ""
                        }
                    }
                )
        # save the updated data
        await self.save_json()
