import base64
import binascii
import ConfigParser
import io


def encodeBase64(s):
    if not s:
        return None

    try:
        return base64.encodestring(s)
    except binascii.Error:
        raise binascii.Error


def decodeBase64(s):
    if not s:
        return None

    try:
        return base64.decodestring(s)
    except binascii.Error:
        raise binascii.Error


def getConfigParameter(data, key, section="DEFAULT"):
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.readfp(io.BytesIO(data))
    return config.get(section, key)
