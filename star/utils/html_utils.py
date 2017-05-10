import urllib


def decode_url(url_str):
    """
    Replaces percent-encoded characters to 
    :param url_str: str
    :return: str
    """
    return urllib.parse.unquote(url_str)
