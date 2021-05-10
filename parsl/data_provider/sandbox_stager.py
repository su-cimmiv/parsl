import paramiko
import os


class SandboxStager(object):
    def __init__(self):
        self._hostname = None,
        self._username = None

    @property
    def hostname(self):
        return self._hostname

    @property
    def username(self):
        return self._username

    @hostname.setter
    def hostname(self, value):
        self._hostname=value

    @username.setter
    def username(self,value):
        self._username = value



    def cp_command(self, src, dst):
        return 'cp -r '+src+' '+dst+'/ '

    def scp_command(self, src, dst):
        return 'scp -r '+self.username+"@"+self.hostname+":~/"+src+" "+dst+"/"

    def ftp_copy(self,src_dir,src_file,dst):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=self.hostname,port=22, username=self.username)
        remote_dir = src_dir
        remote_file = src_file
        dest = dst
        dest_file = src_file
        sftp = ssh_client.open_sftp()
        sftp.get(remote_dir+"/"+remote_file, dest+"/"+remote_file)
        sftp.close()
        ssh_client.close()
