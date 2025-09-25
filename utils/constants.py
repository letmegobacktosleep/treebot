# import built-in packages
import re

# set a datetime string format
DATETIME_STRING_FORMAT = "%Y-%m-%d %H:%M:%S"

# matches the timestamp from # <t:1735689600:R> 
# which would be # 2025-1-1 00:00:00 UTC
PATTERN_TIMESTAMP = re.compile(r"(?<=<t:)(\d+)(?=:?[a-zA-Z]?>)")
# matches digits (greedy)
PATTERN_DIGITS = re.compile(r"[0-9]+")
