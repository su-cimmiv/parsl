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

    def __init__(self):
        self._working_directory = None
        self._workflow_name = ""
        self._app_name = ""
        self._tasks_dep = None

    @property
    def tasks_dep(self):
        return self._tasks_dep

    @property
    def app_name(self):
        return self._app_name

    @property
    def workflow_name(self):
        return self._workflow_name

    @property
    def working_directory(self):
        return self._working_directory

    @working_directory.setter
    def working_directory(self, value):
        self._working_directory = value

    @workflow_name.setter
    def workflow_name(self, value):
        self._workflow_name = value

    @app_name.setter
    def app_name(self, value):
        self._app_name = value

    @tasks_dep.setter
    def tasks_dep(self, value):
        self._tasks_dep = value

    def generateUniqueLabel(self, label):
        """
        Generates a unique name for the scratch directory
        """
        millis = int(round(time.time() * 1000))
        return str(millis) + "-" + label

    def createWorkingDirectory(self):
        self.working_directory = self.generateUniqueLabel(self.app_name)
        wd = self.workflow_name + "/" + self.working_directory if self.workflow_name != "" else self.working_directory
        os.makedirs(wd)

    def pre_execute(self):
        command = ""
        if self.workflow_name != "":
            command += 'cd ' + self.workflow_name + "/" + self.working_directory + "; \n"
        else:
            command += 'cd ' + self.working_directory + "; \n"
        return command

    def count_deps(self, script):
        info = script.split(" ")
        count = 0
        for i in range(len(info)):
            if 'workflow://' in info[i]:
                count += 1
        return count

    def find_task_by_name(self, task_name, tasks_list):
        # For each key-value pair in tasks
        for key, value in tasks_list.items():
            # value in this case is still a key of an internal dict
            # if the value of 'workflow_app_name' is equal to the app_name passed
            # return the key ( task_id)
            if value['workflow_app_name'] == task_name:
                return key

    def resolve_workflow_schema(self, script):
        """
        This method resolve the workflow:// schema
        """

        tasks = json.loads(self.tasks_dep)
        stager = SandboxStager()
        info = script.split(" ")
        script = script.replace("workflow://", "")
        command = ""
        for i in range(len(info)):
            if 'workflow://' in info[i]:
                dep_app_info = info[i].replace("workflow://", "").split("/")
                dep_app_index = self.find_task_by_name(dep_app_info[1], tasks)
                dep_app_wd = tasks[dep_app_index]['working_directory']
                dep_app_wf_name = dep_app_info[0]
                src = dep_app_wf_name + "/" + dep_app_wd if dep_app_wf_name != "" else dep_app_wd
                dst = self.workflow_name + "/" + self.working_directory if self.workflow_name != "" else self.working_directory
                command += stager.cp_command(src, dst)
                command += "\n"
                script = script.replace(dep_app_wf_name + "/" + dep_app_info[1], dep_app_wd)
        command += self.pre_execute()
        command += "\n"
        command += script
        return command

    def define_command(self, script):
        """
        This method prepare the command that must be executed.
        If no workflow:// schema dep is matched the app does not requier any input file
        and the pre_execute method is called.

        If the app requires same input file,
        workflow_schema resolver is
        called for resolve workflow://schema.
        workflow_schema call the stager for the
        import of files from
        the scratches of other task into the scratch of the
        current
        """

        command = ""
        count_dep = self.count_deps(script)
        if count_dep != 0:
            command += self.resolve_workflow_schema(script)
        else:
            command += self.pre_execute()
            command += script
        return command


# the sandbox executor
def sandbox_runner(func, *args, **kwargs):
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

    # create a sandbox passing the name of the scratch directory
    sandbox = Sandbox()

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

    # the workflow_name
    sandbox.workflow_name = kwargs.get('project', "")

    # app name
    sandbox.app_name = kwargs.get("workflow_app_name", "")

    # create a working dir with the sandbox
    sandbox.createWorkingDirectory()

    # workflow schema as workflow:///funcNameUUID
    workflow_schema = "workflow://" + sandbox.workflow_name + "/" + sandbox.app_name

    # tasks dep of the current task
    if 'tasks' in kwargs:
        sandbox.tasks_dep = kwargs.get('tasks', "")
    # Try to run the func to compose the commandline
    try:
        # Execute the func to get the commandline
        executable = func(*args, **kwargs)
        executable = sandbox.define_command(executable)

    except AttributeError as e:
        if executable is not None:
            raise pe.AppBadFormatting("App formatting failed for app '{}' with AttributeError: {}".format(func_name, e))
        else:
            raise pe.BashAppNoReturn(
                "Bash app '{}' did not return a value, or returned None - with this exception: {}".format(func_name, e))

    except IndexError as e:
        raise pe.AppBadFormatting("App formatting failed for app '{}' with IndexError: {}".format(func_name, e))
    except Exception as e:
        raise e

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

        logger.debug("workflow://schema: %s", workflow_schema)

        proc = subprocess.Popen(executable, stdout=std_out, stderr=std_err, shell=True, executable='/bin/bash')
        proc.wait(timeout=timeout)

        return_value = {
            'return_code': proc.returncode,
            'working_directory': sandbox.working_directory,
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
        super().__init__(func, data_flow_kernel=data_flow_kernel, executors=executors, cache=cache,
                         ignore_for_cache=ignore_for_cache)
        self.kwargs = {}

        # We duplicate the extraction of parameter defaults
        # to self.kwargs to ensure availability at point of
        # command string format. Refer: #349
        sig = signature(func)
        for s in sig.parameters:
            if sig.parameters[s].default is not Parameter.empty:
                self.kwargs[s] = sig.parameters[s].default

        # update_wrapper allows sandbox_runner to masquerade as self.func
        # partial is used to attach the first arg the "func" to the sandbox_runner
        # this is done to avoid passing a function type in the args which parsl.serializer
        # doesn't support
        remote_fn = partial(update_wrapper(sandbox_runner, self.func), self.func)
        remote_fn.__name__ = self.func.__name__

        # set __doc__ for sandbox_app
        remote_fn.__doc__ = """sandbox_app"""

        self.wrapped_remote_function = wrap_error(remote_fn)

    def __call__(self, *args, **kwargs):
        """Handle the call to a Bash app.

        Args:
             - Arbitrary

        Kwargs:
             - Arbitrary

        Returns:
                   App_fut

        """

        invocation_kwargs = {}
        invocation_kwargs.update(self.kwargs)
        invocation_kwargs.update(kwargs)

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        app_fut = dfk.submit(self.wrapped_remote_function,
                             app_args=args,
                             executors=self.executors,
                             cache=self.cache,
                             ignore_for_cache=self.ignore_for_cache,
                             app_kwargs=invocation_kwargs)

        return app_fut
