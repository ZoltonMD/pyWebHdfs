import json
import request
import structures


class WebHdfs():
    def __init__(self, host, port=50070, user='hadoop'):
        self.host = host
        self.port = port
        self.user = user
        self.prefix = '/webhdfs/v1'

    def file_status(self, path='/'):
        """Return a file status object that represents the path.

        Args:
            path: The path we want information from

        Returns:
            FileStatus JSON object:
            {
              "FileStatus":
              {
                "accessTime"      : 0,
                "blockSize"       : 0,
                "group"           : "supergroup",
                "length"          : 0,             //in bytes, zero for directories
                "modificationTime": 1320173277227,
                "owner"           : "webuser",
                "pathSuffix"      : "",
                "permission"      : "777",
                "replication"     : 0,
                "type"            : "DIRECTORY"    //enum {FILE, DIRECTORY}
              }
            }
        """
        url = '%s%s?user.name=%s&op=%s' % (self.prefix, path, self.user, 'GETFILESTATUS')
        response = request.push(self.host, self.port, 'GET', url)
        js = json.loads(response)
        return js

    def file_checksum(self, path):
        """Get the checksum of a file.

        Args:
            path:  Given path.

        Returns:
            The checksum object contains:

            self.algorithm: The name of the checksum algorithm.
            self.bytes: The byte sequence of the checksum in hexadecimal
            self.length: The length of the bytes (not the length of the string).
        """
        url = '%s%s?user.name=%s&op=%s' % (self.prefix, path, self.user, 'GETFILECHECKSUM')
        response = request.push(self.host, self.port, 'GET', url)
        js = json.loads(response)
        result = structures.Checksum()
        result.algorithm = js['FileChecksum']['algorithm']
        result.bytes = js['FileChecksum']['bytes']
        result.length = js['FileChecksum']['length']
        return result

    def rename(self, src_path, dst_path):
        """Rename file or directory.

        Args:
            src_path: Current path
            dst_path: New path

        Returns:
            True if operation is successful and False if not.
        """
        url = '%s%s?user.name=%s&op=%s&destination=%s' % (self.prefix, src_path, self.user, 'RENAME', dst_path)
        response = request.push(self.host, self.port, 'PUT', url)
        js = json.loads(response)
        return js['boolean']

    def mkdir(self, path, permission=775):
        """Create directory.
        Has the semantics of Unix 'mkdir -p'.

        Args:
            path: Path to the directory you want to create.
            permission: The permission of a file/directory. <Octal>

        Returns:
            True if successful or False if not.
        """
        url = '%s%s?user.name=%s&op=%s&permission=%s' % (self.prefix, path, self.user, 'MKDIRS', permission)
        response = request.push(self.host, self.port, 'PUT', url)
        js = json.loads(response)
        return js['boolean']

    def list(self, path):
        """Get list of directory's content in WebHDFS JSON format.

        Args:
            path: Given path.

        Returns:
            The client receives a response with a FileStatuses JSON object:
        """
        url = '%s%s?user.name=%s&op=%s' % (self.prefix, path, self.user, 'LISTSTATUS')
        response = request.push(self.host, self.port, "GET", url)
        js = json.loads(response)
        return js['FileStatuses']['FileStatus']

    def ls(self, path):
        """Get list of directory's contents.

        Args:
            path: The path we want information from.

        Returns:
            List of content of target directory.
            For example:

                ['directory1', 'directory2', 'file1', 'file2']
        """
        r_js = self.list(path)
        list_dir = []
        for i in r_js:
            list_dir.append(i['pathSuffix'])
        return list_dir

    def get_rf(self, path):
        """Getting of replication factor.

        Args:
            path: The path we want information from.

        Returns:
            Replication Factor.

        """
        file_status = self.file_status(path)
        rf = file_status['FileStatus']['replication']
        return rf

    def set_rf(self, path, rf):
        """Set replication factor for file.

        Args:
            path: The path we want change RF for.
            rf: Replication factor <SHORT>

        Returns:
            True if changing of replication factor is successful,
            False if not.
        """
        url = '%s%s?user.name=%s&op=SETREPLICATION&replication=%s' % (self.prefix, path, self.user, rf)
        response = request.push(self.host, self.port, 'PUT', url)
        js = json.loads(response)
        return js['boolean']

    def delete(self, path, recursive="false"):
        """

        Args:
            path: The path to file or directory we want delete.

        Returns:
        curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
                              [&recursive=<true|false>]"
        """
        url = '%s%s?user.name=%s&op=DELETE&recursive=%s' % (self.prefix,
                                                            path,
                                                            self.user,
                                                            recursive)
        response = request.push(self.host, self.port, 'DELETE', url)
        js = json.loads(response)
        return js['boolean']

    def exists(self, path):
        """Check existence of a file or directory in the specified path.

        Args:
            path: The path we want information from.

        Returns:
            True if target exists,
            False if target isn't exists.
        """
        r = self.file_status(path)
        return 'FileStatus' in r

    def isdir(self, path):
        """
        Args:
            path: The path we want information from.

        Returns:
            True if is a directory,
            False if isn't a directory.
        """
        r = self.file_status(path)
        return self.exists(path) and r['FileStatus']['type'] == 'DIRECTORY'

    def isfile(self, path):
        """
        Args:
            path: The path we want information from.

        Returns:
            True if target is a file,
            False if target isn't a file.
        """
        r = self.file_status(path)
        return self.exists(path) and r['FileStatus']['type'] == 'FILE'
