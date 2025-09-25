# import 3rd party modules
import discord


async def button_emojis_from_message(message: discord.Message) -> set[str]:
    """
    Gets the emojis of all the buttons
    """
    # helper function
    def get_emoji(component):
        if (
            # doesn't exist?
            component is None or
            # not a button
            component.type.name != "button" or
            # button is disabled
            component.disabled
        ):
            # return nothing
            return None
        # get the emoji as a string
        emoji = component.emoji
        if emoji is None: # doesn't exist
            return None
        elif isinstance(emoji, (discord.PartialEmoji, discord.Emoji)):
            emoji = emoji.name
        elif isinstance(emoji, str):
            pass
        else: # unknown type
            return None
        return emoji

    # create a set (unique values only)
    buttons = set()
    # check if components exist
    components = message.components
    if components is None:
        return None
    # interate through list of components
    for component in components:
        # button - single button
        if isinstance(component, (discord.Button, discord.components.Button)):
            emoji = get_emoji(component=message.button)
            if emoji is not None:
                buttons.add(emoji)
        # action row - multiple buttons
        elif isinstance(component, (discord.ActionRow, discord.components.ActionRow)):
            for child in component.children:
                if isinstance(child, (discord.Button, discord.components.Button)):
                    emoji = get_emoji(component=child)
                    if emoji is not None:
                        buttons.add(emoji)

    return buttons
