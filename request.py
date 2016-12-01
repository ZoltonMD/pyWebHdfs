import httplib
from sys import exit
from urlparse import urlparse


class Error(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Redirect():
    def __init__(self):
        pass

    def redirect_307(self, location, method):
        """Temporaty redirect.

        Args:
            location: HTTP-header "Location"
            method: HTTP-method

        Returns:
            Redirect response
        """
        redirect = urlparse(location)
        conn = httplib.HTTPConnection(redirect.hostname, redirect.port)
        redirect_url = '%s?%s' % (redirect.path, redirect.query)
        conn.request(method, redirect_url)
        res = conn.getresponse()
        data = res.read()
        conn.close()
        return data


def push(host, port, method, url):
    """Function for getting HTTP-response by HTTP-request.

    Args:
        host: hostname or IP address of active Name Node
        port: Web HDFS port number
        method: HTTP method. May be in ['GET', 'PUT', 'POST', 'DELETE']
        url:

    Returns:
        Message-body of HTTP response.
    """
    try:
        if method not in ['GET', 'PUT', 'PASTE', 'DELETE']:
            raise Error('Unknown HTTP-method %s' % method)
    except Error as err:
        print err.value
        exit()

    conn = httplib.HTTPConnection(host, port)
    conn.request(method, url)
    res = conn.getresponse()
    if res.status == 307:
        location = res.getheader('Location')
        conn.close()
        redirect = Redirect()
        data = redirect.redirect_307(location, method)
    elif res.status == 200:
        data = res.read()
        conn.close()
    else:
        data = None
    return data
