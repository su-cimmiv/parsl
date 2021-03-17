class SandboxStager(object):
    
    def cp_command(self, src, dst):
        return 'cp -r '+src+' '+dst+'/ '
