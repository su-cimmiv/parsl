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
        return 'scp -r '+self.username+"@"+self.hostname+":~/"+src+" "+dst
