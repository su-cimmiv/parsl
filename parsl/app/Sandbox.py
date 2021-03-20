from functools import update_wrapper
from functools import partial
from inspect import signature, Parameter
from socket import gethostname, gethostbyname



import parsl

from parsl.app.bash import BashApp
from parsl.app.errors import wrap_error
from parsl.data_provider.files import File
from parsl.dataflow.dflow import DataFlowKernelLoader
from parsl.data_provider.sandbox_stager import SandboxStager

import os
import time
import hashlib
import json
import re



class Sandbox(object):

    def __init__(self, project_name):
        self._working_directory = None
        self._workflow_name = re.sub('[^a-zA-Z0-9 \n\.]', '', parsl.dfk().workflow_name)
        self._tasks_dep = []

        #self._project_name = project_name

    @property
    def tasks_dep(self):
        return self._tasks_dep
        


    @property
    def workflow_name(self):
        return self._workflow_name

    @property
    def working_directory(self):
        return self._working_directory
    @working_directory.setter
    def working_directory(self,value):
        self._working_directory = value
        

    def generateUniqueLabel(self, label): 
        """
        Generates a unique name for the scratch directory 
        """
        millis = int(round(time.time() * 1000))
        return str(millis) + "-" + label

    def createWorkingDirectory(self, label):
        self.working_directory = self.generateUniqueLabel(label)
        wd = self.workflow_name +"/"+self.working_directory
        os.makedirs(wd)

    def pre_execute(self):
         """ 
         If the command does not require files, just move to the task works directory
         """
         return "cd "+self.workflow_name +"/"+self.working_directory+" \n"
         
    def pre_process(self, executable, inputs = []):
        """

        This method resolve the workflow://schema. 
        

        """

        executable = executable.replace(parsl.dfk().SCHEMA,"")
        command = ""
        stager = SandboxStager()
        for i in range(len(inputs)):
            info = inputs[i].replace(parsl.dfk().SCHEMA,"").split("/")
            self.tasks_dep.append(parsl.dfk()._find_task_by_name(info[1]))
            if self.tasks_dep[i]['ip'] == gethostbyname(gethostname()):
                command+=stager.cp_command(info[0]+"/"+self.tasks_dep[i]['app_fu'].result()['working_directory']
                                            ,self.workflow_name+"/"+self.working_directory)
                executable = executable.replace(info[0]+"/"+info[1],self.tasks_dep[i]['app_fu'].result()['working_directory'])
                command+="\n"
        command+=self.pre_execute()
        command+=executable
        
        return command

    def define_command(self, executable,inputs=[]):
        """
        This method prepare the command that must be executed.
        If inputs len is 0, the app does not requier any input file
        and the pre_execute method is called.

        If the app requires same input file, pre_process method is
        called for resolve workflow://schema.
        pre_process call the stager for the import of files from 
        the scratches of other task into the scratch of the 
        current


        """
        command = ""
        if len(inputs)==0:
            command+=self.pre_execute()
            command+=executable
        else:
            command += self.pre_process(executable,inputs)

        return command
    
    

#the sandbox executor
def sandbox_executor(func, *args, **kwargs):
    """Executes the supplied function with *args and **kwargs to get a
    command-line to run, and then run that command-line using bash.
    """
    import os
    import time
    import subprocess
    import logging
    import parsl.app.errors as pe
    from parsl import set_file_logger
    from parsl.utils import get_std_fname_mode
    from parsl.data_provider.files import File

    import json

    #create a sandbox passing the name of the scratch directory
    sandbox = Sandbox("")

    logbase = "/tmp"
    format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    # make this name unique per invocation so that each invocation can
    # log to its own file. It would be better to include the task_id here
    # but that is awkward to wire through at the moment as apps do not
    # have access to that execution context.
    t = time.time()

    logname = __name__ + "." + str(t)
    logger = logging.getLogger(logname)
    set_file_logger(filename='{0}/bashexec.{1}.log'.format(logbase, t), name=logname, level=logging.DEBUG,
                    format_string=format_string)

    func_name = func.__name__

    executable = None
    
    #create a working dir with the sandbox
    sandbox.createWorkingDirectory(func_name)
    #app name retuned 
    app_name = kwargs.get("workflow_app_name","")
    # workflow schema as workflow:///funcNameUUID
    workflow_schema = "workflow://"+sandbox.workflow_name+"/"+app_name
    
    # Try to run the func to compose the commandline
    try:

        # Execute the func to get the commandline
        executable = func(*args, **kwargs)

    except AttributeError as e:
        if executable is not None:
            raise pe.AppBadFormatting("App formatting failed for app '{}' with AttributeError: {}".format(func_name, e))
        else:
            raise pe.BashAppNoReturn("Bash app '{}' did not return a value, or returned None - with this exception: {}".format(func_name, e))

    except IndexError as e:
        raise pe.AppBadFormatting("App formatting failed for app '{}' with IndexError: {}".format(func_name, e))
    except Exception as e:
        raise e
    
    executable = sandbox.define_command(executable,kwargs.get('inputs',[]))
    
    
    # Updating stdout, stderr if values passed at call time.

    def open_std_fd(fdname):
        # fdname is 'stdout' or 'stderr'
        stdfspec = kwargs.get(fdname)  # spec is str name or tuple (name, mode)
        if stdfspec is None:
            return None

        fname, mode = get_std_fname_mode(fdname, stdfspec)
        try:
            if os.path.dirname(fname):
                os.makedirs(os.path.dirname(fname), exist_ok=True)
            fd = open(fname, mode)
        except Exception as e:
            raise pe.BadStdStreamFile(fname, e)
        return fd

    std_out = open_std_fd('stdout')
    std_err = open_std_fd('stderr')
    timeout = kwargs.get('walltime')
    
    if std_err is not None:
        print('--> executable follows <--\n{}\n--> end executable <--'.format(executable), file=std_err, flush=True)

    return_value = None
    try:
        cwd = None
        cwd = os.getcwd() #current working dir
        logger.debug("workflow://schema: %s", workflow_schema)
        print(executable)
        proc = subprocess.Popen(executable,stdout=std_out, stderr=std_err, shell=True, executable='/bin/bash')
        proc.wait(timeout=timeout)

        return_value = {
        'workflow_schema': workflow_schema,
        'return_code' : proc.returncode,
        'working_directory':sandbox.working_directory,
        'workflow_app_name':app_name
        }

    except subprocess.TimeoutExpired:
        raise pe.AppTimeout("[{}] App exceeded walltime: {}".format(func_name, timeout))

    except Exception as e:
      raise pe.AppException("[{}] App caught exception with return value: {}"
                              .format(func_name, json.dumps(return_value)), e)


    if proc.returncode != 0:
        raise pe.BashExitFailure(func_name, proc.returncode)

    # TODO : Add support for globs here


    missing = []
    for outputfile in kwargs.get('outputs', []):
        fpath = outputfile.filepath

        if not os.path.exists(fpath):
            missing.extend([outputfile])

    if missing:
        raise pe.MissingOutputs("[{}] Missing outputs".format(func_name), missing)
    return return_value

class SandboxApp(BashApp):
       def __init__(self, func, data_flow_kernel=None, cache=False, executors='all', ignore_for_cache=None):
        super().__init__(func, data_flow_kernel=data_flow_kernel, executors=executors, cache=cache, ignore_for_cache=ignore_for_cache)
        self.kwargs = {}

        

        # We duplicate the extraction of parameter defaults
        # to self.kwargs to ensure availability at point of
        # command string format. Refer: #349
        sig = signature(func)

        for s in sig.parameters:
            if sig.parameters[s].default is not Parameter.empty:
                self.kwargs[s] = sig.parameters[s].default

        # update_wrapper allows sandbox_executor to masquerade as self.func
        # partial is used to attach the first arg the "func" to the sandbox_executor
        # this is done to avoid passing a function type in the args which parsl.serializer
        # doesn't support
        remote_fn = partial(update_wrapper(sandbox_executor, self.func), self.func)
        remote_fn.__name__ = self.func.__name__

        #set __doc__ for sandbox_app
        remote_fn.__doc__ = """sandbox_app"""

        self.wrapped_remote_function = wrap_error(remote_fn)