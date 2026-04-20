# import built-in packages
import json
# import 3rd party packages
import discord
# import utils
from utils.json import BotConfigFile
from utils.constants import PATTERN_DIGITS

async def guild_id_from_interaction(
    interaction: discord.Interaction
) -> int | None:
    """
    get the guild_id from the interaction
    """
    # ensure that the guild_id exists
    if not interaction.guild_id:
        await interaction.response.send_message(
            content="Command can only be used in guilds.",
            ephemeral=True
        )
        return None
    # return the guild_id
    return interaction.guild_id

async def util_modify_config(
    interaction: discord.Interaction,
    config_class: BotConfigFile,
    category: str,
    config_values: list[tuple]
) -> None:
    """
    modifies a configuration using key:value pairs
    """
    # ensure that the guild_id exists
    guild_id = await guild_id_from_interaction(interaction=interaction)
    if guild_id is None:
        # command was not used inside a guild
        return None
    # fetch the config
    config = await config_class.get_data(guild_id, category)
    has_changed = False
    # set the values
    for key, value in config_values:
        if value is not None:
            config[key] = value
            has_changed = True
    # save the new values
    content = ""
    if has_changed:
        await config_class.set_data(guild_id, category, config)
        content += f"`{category}` has been successfully changed!\n"
    else:
        content += f"Current `{category}` config:"
    # respond to the interaction
    content += f"```json\n{json.dumps(config, indent=4)}\n```"
    await interaction.response.send_message(
        content=content,
        ephemeral=True
    )
