import ssl

import httpx


def httpx_client(cafile=None):
    """Get an httpx client with SSL cacert if specified otherwise default
    
    See https://www.python-httpx.org/advanced/ssl/
    """
    if cafile:
        ctx = ssl.create_default_context(cafile=cafile)
        client = httpx.Client(verify=ctx)
    else:
        client = httpx.Client()
    return client
