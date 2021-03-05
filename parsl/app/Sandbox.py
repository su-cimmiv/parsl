from functools import update_wrapper
from functools import partial
from inspect import signature, Parameter

from parsl.app.bash import BashApp
from parsl.app.errors import wrap_error
from parsl.data_provider.files import File
from parsl.dataflow.dflow import DataFlowKernelLoader

import os
import time
import datetime
import pwd

from uuid import uuid4


class Sandbox(object):

    def __init__(self, project_name):
        self._working_directory = None
        self._project_name = project_name

  
    @property
    def working_directory(self):
        return self._working_directory
    @working_directory.setter
    def working_directory(self,value):
        self._working_directory = value

    @property
    def project_name(self):
        return self._project_name
    @project_name.setter
    def project_name(self, value):
         self._project_name = value

    def generateUniqueLabel(self, label):
        return uuid4().hex+label

    def createWorkingDirectory(self, label):
        self.working_directory = self.generateUniqueLabel(label)
        wd = self._project_name+"/"+ self.working_directory
        os.makedirs(wd)
    
    def workflowSchemaResolver(self,inputs=[]):
        for i in range(len(inputs)):
            if isinstance(inputs[i],File):
                inputs[i].filepath = inputs[i].filepath.replace("workflow://","")
            else:
                inputs[i] = inputs[i].replace("workflow://","")
        return inputs
    
    def pre_run(self, executable, inputs = [], removedir=False):
        #go in the workflowName Folder
        command = "cd ../ \n"
        #for each input file resolve workflow:// schema
        inFile = self.workflowSchemaResolver(inputs)
        #copy files to current directory
        for i in range(len(inFile)):
            if isinstance(inFile[i],File):
                command+="cp --parents "+inFile[i].filepath+" "+self.working_directory+"/ \n"
            else:
                command+="cp --parents "+inFile[i]+" "+self.working_directory+"/ \n"
        #go into the working dir of the app
        command+='cd '+self.working_directory+"\n"
        #resolve the workflow schema in the command
        _executable = executable.replace("workflow://","")
        command+=_executable+"\n\n"
        #after running, delete imported folders if the flag is set to True ( dafault is set to False )
        dirname = None
        path = None
        if removedir == True:
            for i in range(len(inFile)):
                if isinstance(inFile[i],File):
                    path = inFile[i].filepath
                else:
                    path = inFile[i]
                command+="rm -r "+dirname[0]+"/ \n"
        else:
            for i in range(len(inFile)):
                if isinstance(inFile[i],File):
                    path = inFile[i].filepath
                else:
                    path = inFile[i]
                dirname = path.split("/")
                command+="\nmv "+dirname[0]+"/ "+dirname[0]+"_old/ \n"
        
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
    
    #the workflow name
    project = kwargs.get('project',"")
	#set the project name
    sandbox.project_name = project
    #create a working dir with the sandbox
    sandbox.createWorkingDirectory(func_name)
    wdPath = sandbox.project_name+"/"+sandbox.working_directory
    # workflow schema as workflow:///funcNameUUID
    workflow_schema = "workflow://"+sandbox.working_directory
    
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
    
    #if there is an 'inputs' field in kwargs and it is not None, create the command using the sandbox
    if 'inputs' in kwargs and kwargs.get('inputs',[]) is not None:
        #update the kwargs for the app
        inputs = kwargs.get("inputs",[])
        kwargs.update(inputs=inputs)
        executable = sandbox.pre_run(executable,inputs)

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
        

        #Pass to Popen even the working direcotory created with the sandbox
        proc = subprocess.Popen(executable, cwd = wdPath,stdout=std_out, stderr=std_err, shell=True, executable='/bin/bash')
        proc.wait(timeout=timeout)

        return_value = {
        'workflow_schema': workflow_schema,
        'return_code' : proc.returncode,
        }

        if cwd is not None:
            os.chdir(cwd)

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
        self.wrapped_remote_function = wrap_error(remote_fn)
