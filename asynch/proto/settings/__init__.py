import logging

from asynch.proto.settings.available import settings as available_settings
from asynch.proto.streams.buffered import BufferedWriter

logger = logging.getLogger(__name__)


class SettingsFlags:
    IMPORTANT = 0x1
    CUSTOM = 0x2


async def write_settings(
    writer: BufferedWriter,
    settings: dict,
    settings_as_strings,
    flags: int,
):
    for setting, value in (settings or {}).items():
        # If the server support settings as string we do not need to know
        # anything about them, so we can write any setting.
        if settings_as_strings:
            await writer.write_str(setting)
            await writer.write_uint8(flags)
            await writer.write_str(str(value))

        else:
            # If the server requires string in binary,
            # then they cannot be written without type.
            setting_writer = available_settings.get(setting)
            if not setting_writer:
                logger.warning("Unknown setting %s. Skipping", setting)
                continue
            await writer.write_str(
                setting,
            )
            await setting_writer.write(writer, value)

    await writer.write_str("")  # end of settings
