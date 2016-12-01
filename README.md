# pyWebHdfs

pyWebHdfs is the Python module for interaction with HDFS (Hadoop Distributed File System).
Besides standard WebHDFS operations the module contents UNIX-like operations such us “ls”, “cp”, “mkdir”, etc.

## How to use
```python
from pyWebHdfs import WebHdfs
hdfs = WebHdfs("localhost")
hdfs.ls("/path/to/dir")
```

## Helpful links
[More documentation about WebHDFS REST API](https://hadoop.apache.org/docs/r1.0.4/webhdfs.html)


