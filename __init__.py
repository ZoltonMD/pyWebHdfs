"""
pyWebHdfs - module for working with WebHdfs in python

>>> from pyWebHdfs import WebHdfs
>>> hdfs = WebHdfs("localhost")
>>> ...
>>> hdfs.ls_h("/home")

"""

__version__ = '0.1'

from .webhdfs import *