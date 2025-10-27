# import built-in packages
import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field
# import 3rd party packages
import pytz
import discord
from discord.ext import commands

# set up the logger
logger = logging.getLogger(__name__)

async def util_fetch_channel(
    bot: commands.Bot,
    channel_id: int
) -> discord.TextChannel | None:
    """
    Fetches a channel
    """
    # fetch the channel
    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
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

    return channel

async def util_send_message_in_channel(
    bot: commands.Bot,
    channel_id: int,
    content: Optional[int] = None,
    files: Optional[list[discord.File]] = None
) -> discord.Message | None:
    """
    Fetches a channel then attempts to send the message.
    """
    # fetch the channel
    channel = await util_fetch_channel(
        bot=bot,
        channel_id=channel_id
    )
    if channel is None:
        return None

    # skip if no permission to send messages
    permissions = channel.permissions_for(bot.user)
    if not permissions.send_messages:
        # logger.warning(f"No permission to send messages in channel %d", channel_id)
        return None

    # send the message
    try:
        message = await channel.send(content=content, files=files)
        return message
    except discord.NotFound as e:
        logger.warning(f"The channel could not be found: {channel_id}.\n{e}")
        return None
    except discord.Forbidden as e:
        logger.warning(f"Insufficient permissions to send messages in channel: {channel_id}.\n{e}")
        return None
    except ValueError as e:
        logger.warning(f"The files or embeds list is not of the appropriate size: {channel_id}.\n{e}")
        return None
    except ValueError as e:
        logger.warning(f"You specified both file and files, or you specified both embed and embeds, or the reference object is not a Message, MessageReference or PartialMessage: {channel_id}.\n{e}")
        return None
    except discord.HTTPException as e:
        logger.warning(f"Failed to retrieve the channel: {channel_id}.\n{e}")
        return None

@dataclass
class DummyMessage:
    """
    dummy message storing the created_at variable
    """
    created_at: datetime = field(default_factory=lambda: datetime.now(pytz.utc))
