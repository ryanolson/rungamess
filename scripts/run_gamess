#!/usr/bin/env python
#
# Python program to run MPI programs.
#

import optparse
formatter = optparse.IndentedHelpFormatter(max_help_position=40)
parser = optparse.OptionParser(formatter=formatter)

env = {}
env["GMS_CCHEM"] = "1"
# default environment variables
env["I_MPI_DEVICE"] = "sock"
#env["I_MPI_DEVICE"] = "rdma"
env["I_MPI_WAIT_MODE"] = "enable"
env["I_MPI_NETMASK"] = "ib0"
# env["I_MPI_PIN"] = "1"
#env["I_MPI_DEBUG"] = "4"
#env["CUDA_CACHE_DISABLE"] = "1"
#env["CUDA_CACHE_PATH"] = "/tmp/$USER-ptx-cache"

# set defaults here
parser.set_defaults(gamess="$PWD/gamess.serial.x")
parser.set_defaults(gamess_ericfmt="$PWD/auxdata/ericfmt.dat")
#parser.set_defaults(mpi="mpich")
parser.set_defaults(wdir="$HOME/scratch/$USER")
parser.set_defaults(gamess_server=False)
#parser.set_defaults(rsh="ssh") # enable to use rsh to set up job

# PBS resource list template
resources = "nodes=$nodes:ppn=$ppn:gpus=$gpus"

import math
import sys
import tempfile
import os
import subprocess
from subprocess import Popen
import re
from string import Template

import traceback


def main():
    global parser
    global options
    qsysList = [Shell, PBS, SGE, LSF, LoadLeveler]
    mpi_classes = [MPICH, OpenMPI, BlueGene, XT]
    

    # usage and description
    parser.set_usage("%prog [options] input...")
    parser.set_description("Submit or job an MPI program.")
    
    # parse options
    addOptions(parser, qsysList, mpi_classes)

    arglen = len(sys.argv)
    if "--" in sys.argv: arglen = sys.argv.index("--") + 1
    (options, args) = parser.parse_args(sys.argv[1:arglen])
    options.gargs = sys.argv[arglen:]

    # initialize program, qsys, mpi, and global options

    setOptions(options, qsysList, mpi_classes)

    # input files have to be specified
    if not args:
        parser.error("input(s) must be specified.")
        sys.exit(1)
        
    # submit or job jobs
    for input in args:
        try:
            job = GamessJob(input, options.qsys.jobid)
            (cmd, script) = options.qsys.getScript(job)

            if options.noop: print script
            else:
                fh = opentemp(prefix="%s." % (job.name), suffix="."+options.qsys.key)
                fh.write(script)
                fh.close()
                p = Popen(cmd + " " + fh.name, shell=True)
                sts = os.waitpid(p.pid, 0)
                #os.system(cmd + " " + script) 


        except Exception, e:
            traceback.print_exc()

    sys.exit(0)



def addOptions(parser, qsysList, mpi_classes):
    # common options
    parser.add_option("-N", "--nodes", type="int", help="number of nodes", metavar="X")
    parser.add_option("-p", "--ppn", type="int", help="processors per node", metavar="X")
    parser.add_option("-n",   "--np", type="int", help="number of processes", metavar="X")
    parser.add_option("-t", "--threads", type="int", help="number of threads", metavar="X")
    parser.add_option("", "--gpus", type="int", help="number of gpus", metavar="X")
    parser.add_option("", "--stack",type="int", help="stacksize")
    parser.add_option("-d", "--wdir", help="working directory", metavar="X")
    parser.add_option("-k", "--keep", action="store_true", help="keep work files")
    parser.add_option("-o", "--output", help="output", metavar="X")
    parser.add_option("-e", "--error", help="error", metavar="X")
    parser.add_option("-E", "--env", action="append", help="environment", metavar="X")
    parser.add_option("-V", "--vars", action="store_true", help="export all vars", metavar="X")
    parser.add_option("", "--serial", action="store_true", help="serial")
    
    parser.add_option("-i", "--interactive", action="store_true", help="interactive")

    parser.add_option("-v", "--verbose", action="store_true", help="verbose")
    parser.add_option("-z", "--noop", action="store_true", help="no op")
    parser.add_option("-s", "--sh", help="shell", default="/bin/bash")
    parser.add_option("-r", "--rsh", help="remote shell")

    parser.add_option("-P", "--path", help="path")
    parser.add_option("-L", "--libpath", help="Library path")

    MPI.add_options(mpi_classes, parser)
    
    help = "Queue System: " + ", ".join(["%s - %s" % (c.key, c.name) for c in qsysList])
    parser.add_option("-Q", "--qsys", choices=[c.key for c in qsysList],
                      help=help, metavar="X")
    parser.add_option("", "--qcmd", help="queue command")
    parser.add_option("", "--qarg", help="queue arguments")
    parser.add_option("-w", "--wtime", help="wall clock time", metavar="X")
    parser.add_option("-q", "--queue", help="queue")
    parser.add_option("-A", "--account", help="account")
    parser.add_option("-J", "--name", help="job name")
    
    parser.add_option("", "--valgrind", action="store_true", help="run valgrind")
    parser.add_option("-g", "--debug", action="store_true", help="enable debug")
    parser.add_option("-G", "--debugger", choices=["gdb", "idb", "tv", "dbx"], default="gdb",
                      help="debugger:gdb, idb, totalview, dbx")
    parser.add_option("", "--gcmd", help="debugger command")
    
    # Gamess options
    parser.add_option("-X", "--gamess",
                      help="gamess cmd", metavar="X")
    parser.add_option_group(GamessJob.getOptionGroup(parser))

    ## DDI
    parser.add_option("", "--ddi", action="store_true", help="ddi run", metavar="X")

    # cchem options
    parser.add_option("", "--cchem", help="CCHEM options")

    for cls in qsysList + mpi_classes:
        group = cls.getOptionGroup(parser)
        if group: parser.add_option_group(group)


    
def setOptions(options, qsysList, mpi_classes):
    global env
    qsys = None
    mpi = None

    if options.path:
	path = os.getenv("PATH")
	path += ":" + options.path
	os.putenv("PATH", path)
	env["PATH"] = path

    if options.libpath:
	path = options.libpath
	path += ":" + os.getenv("LD_LIBRARY_PATH")
	os.putenv("LD_LIBRARY_PATH", path)
	env["LD_LIBRARY_PATH"] = path

    if options.stack:
        env["OMP_STACK_SIZE"] = options.stack*1024
        env["KMP_STACKSIZE"] = options.stack*1024
        env["XLMSOPTS"] = "stack="+str(options.stack*1024)
        env["MP_STACK_SIZE"] =  options.stack*1024
        
    if options.threads:
	env["OMP_NUM_THREADS"] = options.threads
	env["NUM_THREADS"] = options.threads
    if options.bg_flat_profile: env["FLAT_PROFILE"] = "yes"
    if options.xt_symm_heap: env["XT_SYMMETRIC_HEAP"] = options.xt_symm_heap

    if options.cchem: env["CCHEM"] = options.cchem

    if options.hostlist: options.hostlist = options.hostlist.split(',')
    if options.hostlist and not options.nodes:
	options.nodes = len(options.hostlist)

    options.pbs = options.pbs or []
    options.sge = options.sge or []

    if not options.np:
	if options.nodes and options.ppn:
	    options.np = options.nodes*options.ppn
	elif options.nodes: options.np = options.nodes
	elif options.ppn: options.np = options.ppn

    if options.np and options.ddi:
	options.np *= 2

    options.serial = not (options.np or options.hostfile)
    if not (options.mpi or options.serial):
	options.mpi = MPI.cmd

    if options.env:
        for kv in options.env:
            (k,v) = kv.split("=", 2)
            env[k] = v

    options.env = env    
    
    if options.qsys: 
        for qsysClass in qsysList:
            if qsysClass.key == options.qsys:
                options.qsys = qsysClass()
                break
    else: options.qsys = Shell()

    if options.mpi:
	mpi = None
	for C in mpi_classes:
            if C.key == options.mpi:
                mpi = C()
                break
	if not mpi:
	    if not options.mpicmd: options.mpicmd = options.mpi
	    for C in mpi_classes:
		if C.matches(options.mpicmd):
		    mpi = C()
		    break
	assert mpi, "Undefined MPI: %s" % options.mpi
	options.mpi = mpi

    if options.debug:
        options.interactive = True
        if not options.gcmd:
            if options.debugger == "gdb": options.gcmd = "gdb"
            elif options.debugger == "idb": options.gcmd = "idb"
            elif options.debugger == "tv": options.gcmd = "totalview"
            elif options.debugger == "dbx": options.gcmd = "dbx"


# Qsys class
class QSys:
    cmd = None
    arg = None
    
    def getOptionGroup(parser):
        return None 
    getOptionGroup = staticmethod(getOptionGroup)

    def __init__(self):
        global options
        if options.qcmd: self.cmd = options.qcmd
        if options.qarg: self.arg = options.qarg

    def getScript(self, job, stdout = None):
        global options

        script = ""

	if not job.hostlist: job.hostlist = options.hostlist or []
	if not job.hostfile: job.hostfile = options.hostfile

	# #script += "set -x\n"
	# script += "echo $(hostname)\n"
	# script += "mkdir -p %s\n" % job.wdir

	hosts = None
	hostfile = None

	if job.hostfile: hosts = "$(cat %s | awk '{print $1}' | grep -v '^#')" % job.hostfile
	if job.hostlist: hosts = " ".join(job.hostlist)

        if hosts and options.rsh:
            script += "for host in %s; do\n" % (hosts)
            script += "%s $host \"%s\";\n" % (options.rsh, job.getProlog("\\\\\\"))
            script += "done\n"
	else:
	    script += "%s\n" % (job.getProlog("\\\\\\"))
        script += "\n"


	if job.hostlist:
	    job.hostfile = job.wdir + "/machinefile"
	    script += "echo \"%s\" > %s\n" % ("\n".join(job.hostlist), job.hostfile)
	    job.hostlist = ()

	if job.hostfile:
	    job.hostfile = os.path.abspath(job.hostfile)
	    hosts = "$(cat %s | awk '{print $1}' | grep -v '^#')" % job.hostfile
	    hostfile = os.path.join(job.wdir, "machines")

        script += "\n"


        if not hostfile:
	    script += "# no hostfile/hostlist\n"
            script += job.getProlog()
        else:
	    script += "hostfile=%s\n" % hostfile
            hostfile = "$hostfile"
	    filt = "cat"
	    if options.nodes: filt = "sort | uniq"
            if options.ppn: filt += "| sed '%s'" % ((options.ppn-1)*"p;") 
	    script += "cat %s | %s > %s\n" % (job.hostfile, filt, hostfile)
	    job.hostfile = hostfile
	    if options.ddi:
		script += "cat %s %s > %s\n" % (hostfile, hostfile, hostfile + ".ddi")
		script += "hostfile=\"$hostfile.ddi\"\n"
            #script += "cat $hostfile\n"

        script += "\n"

	# generate input files
	for inp in job.files():
	    if hosts and options.rsh:
		script += "for host in %s; do\n" % (hosts)
		script += "echo '%s' | %s $host \"cat > %s\";\n" % (inp.string, options.rsh, inp.name)
		script += "done\n"
	    else:
		script += "echo '%s' | cat > %s\n" % (inp.string, inp.name)
        script += "\n"

        script += "cd %s\n" % (job.wdir)

        if options.serial:
            cmd = job.cmd
            args = job.args
            if options.debug:
                cmd = options.gcmd + " "  + " ".join(options.gargs) + " " + cmd
	    if options.valgrind:
		cmd = "valgrind -v " + cmd
                
        else:
            initCmd = options.mpi.getInitCmd(job)
            if initCmd: script += "%s\n\n" % (initCmd)
            cmd = options.mpi.getCmd()
            args = options.mpi.getArgs(job)

        if args: cmd += " %s" % (args)
	if stdout: cmd += " >%s 2>&1" % stdout
        script += cmd + "\n\n"

	epilog = job.getEpilog("\\\\\\")
	if epilog:
	    if hosts and options.rsh:
		script += "for host in %s; do\n" % (hosts)
		script += "%s $host \"%s\";\n" % (options.rsh, epilog)
		script += "done\n"
	    else:
		script += epilog

	script += "\n"

        return script


# Shell qsys
class Shell(QSys):
    cmd = "/bin/sh"
    key = "sh"
    jobid = "$$"
    name = "Shell"

    def getScript(self, job):
        global options
        
        script = "#/bin/sh\n"
        if options.stack: script += "ulimit -s %i\n" % (options.stack)

        for (k,v) in job.env.items():
            script += "export %s=\"%s\"\n" %(k,v)
        script += "\n"

        script += QSys.getScript(self, job)
        
        return (self.cmd, script)
        
    
# LoadLeveler qsys
class LoadLeveler(QSys):
    cmd = "llsubmit"
    key = "ll"
    jobid = "$LOADL_JOB_NAME"
    name = "LoadLeveler"
    
    def getScript(self, input):
        global options

        script = "#@job_name = %s\n" % (job.name)

        script += "#@job_type = MPICH\n"
        if options.np: script += "#@num_tasks = %i\n" % (options.np)

        if options.wtime: script += "#@wall_clock_limit = %s\n" % (options.wtime)
        if job.wdir: script += "#@initialdir = %s\n" % (job.wdir)

        # job enviroment
        for (name, value) in job.env.items():
            script += "#@environment = %s=%s\n" % (name, value)
            
        script += QSys.getScript(self, job)

        script += "\n"
        script += "#@queue\n"

        return (self.cmd, script)

# PBS qsys
class PBS(QSys):
    key = "pbs"
    name = "PBS"
    cmd = "qsub"
    jobid = "$PBS_JOBID"
    name = "PBS"

    def __init__(self):
        global options
     	if options.qarg: self.cmd += (" " + options.qarg)
        if options.interactive: self.cmd += " -I"
        
    def getOptionGroup(parser):
        group = optparse.OptionGroup(parser, "PBS options")
        group.add_option("", "--pbs", action="append",
                         help="PBS Resource List", metavar="X")
        return group
    getOptionGroup = staticmethod(getOptionGroup)

    def getScript(self, job):
        global options

        script = "#!%s\n" % options.sh
        script += "#PBS -S %s\n" % options.sh
        job.hostfile = "/$PBS_NODEFILE"
        
        if options.account: script += "#PBS -A %s\n" % (options.account)
        if options.queue: script += "#PBS -q %s\n" % (options.queue)

        # resource list
	ppn = None
	if options.ppn: ppn = options.ppn
	if options.threads: ppn = options.threads*(ppn or 1)
	d = dict()
	d["ppn"] = ppn or ""
	d["nodes"] = options.nodes or ""
	d["gpus"] = options.gpus or ""
	r = Template(resources).substitute(d)
	r = ":".join([s for s in r.split(":") if not s.endswith("=")])
	script += "#PBS -l %s\n" % r

        for d in options.pbs or []:
            script += "#PBS %s\n" % (d)
            
        if options.wtime: script += "#PBS -l walltime=%s\n" % (options.wtime)

        # if not options.interactive:
	#     script += "#PBS -k oe\n"
        #     script += "#PBS -o %s\n" % (job.output)
	#     script += "#PBS -e %s\n" % (job.error)
	#     if (job.output == job.error): script += "#PBS -joe\n"
        
        # job environment
        if options.vars: script += "#PBS -V\n"
        for (name, value) in job.env.items():
            script += "export %s=\"%s\"\n" % (name, value)

        script += QSys.getScript(self, job, job.output)
        
        return (self.cmd, script)
                


# SGE qsys
class SGE(QSys):
    key = "sge"
    name = "SGE"
    cmd = "qsub"
    jobid = "$JOB_ID"
    name = "SGE"

    def __init__(self):
        global options
     	if options.qarg: self.cmd += (" " + options.qarg)
        if options.interactive: self.cmd += " -I"
        
    def getOptionGroup(parser):
        group = optparse.OptionGroup(parser, "SGE options")
        group.add_option("", "--sge", action="append",
                         help="SGE directives", metavar="X")
        return group
    getOptionGroup = staticmethod(getOptionGroup)

    def getScript(self, job):
        global options

        script = "#$ -S /bin/sh\n"
        
        job.hostfile = "/$TMPDIR/machines"
        
        if options.account: script += "#$ -A %s\n" % (options.account)
        if options.queue: script += "#$ -q %s\n" % (options.queue)

	pe = None
	if options.mpi:
	    pe = { MPICH.key : "mpich", OpenMPI.key : "orte" }.get(options.mpi.key)
        #script += "#$ -pe %s %i\n" % (pe, options.np or 1)
	script += "#$ -pe %s %i\n" % ("ddi", options.nodes or options.np or 1)

        if not options.interactive:
            script += "#$ -o %s\n" % (job.output)
	    script += "#$ -e %s\n" % (job.error)

        if options.vars: script += "#$ -V\n"
	script += "\n"
        
	script += "\n".join(["#$ %s" % directive for directive in options.sge or []])
	script += "\n"
	script += "\n"

        # job environment
        for (name, value) in job.env.items():
            script += "export %s=\"%s\"\n" % (name, value)
	script += "\n"

        script += QSys.getScript(self, job)
        
        return (self.cmd, script)



# LSF qsys
class LSF(QSys):
    key = "lsf"
    name = "LSF"
    cmd = "bsub"
    jobid = "$LSB_JOBID"
    name = "LSF"
    
    def getScript(self, job):
        script = ""
        script += "#BSUB -J %s\n" % (job.name)
        if options.queue: script += "#BSUB -q %s\n" % (options.queue)
        if options.np: script += "#BSUB -n %i\n" % (options.np)
        if options.wtime: script += "#BSUB -W %s\n" % (options.wtime)
	script += "#BSUB -o %s\n" % (job.output)
	script += "#BSUB -e %s\n" % (job.error)
        #if options.user: script += "#BSUB -u %s\n" % (options.user))

        script += QSys.getScript(self, job)
                
        return (self.cmd + " < ", script)
    

class MPI:
    name = "MPI"
    cmd = "mpiexec"

    def getOptionGroup(parser):
        return None
    getOptionGroup = staticmethod(getOptionGroup)
    
    def matches(cmd, regex = None):
	if not regex: return False
	p = Popen(cmd, stdout = subprocess.PIPE, 
        stderr = subprocess.STDOUT, shell = True)
	p.wait()
	while True:
	    line = p.stdout.read()
	    if not line: break
	    if re.search(regex, line): return True
	return False
    matches = staticmethod(matches)

    def __init__(self):
        global options
        if options.mpicmd: self.cmd = options.mpicmd

    def getInitCmd(self, job):
        return None

    def getCmd(self):
        return self.cmd

    def getArgs(self):
        return None

    def add_options(mpi_classes, parser):
	help = "MPI: "
	help += ", ".join(["%s - %s" % (c.key, c.name) for c in mpi_classes])
	parser.add_option("-m", "--mpi", dest="mpi", help=help)
	parser.add_option("", "--mpicmd", help="mpi command")
	parser.add_option("-f", "--hostfile", help="hostfile")
	parser.add_option("-l", "--hostlist", help="hostlist")
    add_options = staticmethod(add_options)

    
# BlueGene job
class BlueGene(MPI):
    key = "bg"
    cmd = "mpirun"
    name = "Blue Gene"
    
    def getOptionGroup(parser):
        group = optparse.OptionGroup(parser, "Blue Gene options")
        group.add_option("", "--bg-mode", choices=["co", "vn", "smp"],
                         help="Blue Gene mode: co, vn, or smp", metavar="X")
        group.add_option("", "--bg-partition", type="string",
                          help="Blue Gene partition", metavar="X")
        group.add_option("", "--bg-host", type="string",
                          help="Blue Gene host", metavar="X")
        group.add_option("", "--bg-timeout", type="string",
                          help="Blue Gene timeout", metavar="X")
        group.add_option("", "--bg-flat-profile", action="store_true",
                         default=False, help ="FLAT_PROFILE")
        return group
    getOptionGroup = staticmethod(getOptionGroup)

    def __init__(self):
        global options
        
        options.gamess_envfile = True

        if options.mpicmd: self.cmd = options.mpicmd
        
        if not options.ppn:
            options.ppn = 1
            if options.bg_mode == "smp":
                options.ppn = 4
            if options.bg_mode == "vn":
                options.ppn = 2
        if not options.nodes:
            options.nodes = 1
            if options.np:
                options.nodes = math.ceil(float(options.np)/options.ppn)
        if not options.np: options.np = options.nodes * options.ppn

#         if mpi == BlueGene: script += "#@job_type = bluegene\n"
        
#         if mpi == BlueGene:
#             if mpi.partition: script += "#@bg_partition = %s\n" % (mpi.partition)
#             if options.np: script += "#@bg_size = %s\n" % (options.np)

                                                
    def getArgs(self, job):
        global options
        args = ""
        
        if options.bg_mode:
            args += " -mode %s" % (options.bg_mode.upper())
        if options.bg_partition:
            args += " -partition %s" % (options.bg_partition)
        if options.bg_host:
            args += " -host %s" % (options.bg_host)
        if options.bg_timeout:
            args += " -timeout %s" % (options.bg_timeout)
       
        args += " -np %s" % (options.np)
        
        if job.wdir: args += " -cwd %s" % (job.wdir)
        for (k, v) in job.env.items():
            args += " -env \"%s=%s\"" % (k, v)
        if job.cmd: args += " %s" % (job.cmd)
        if job.args: args += " %s" % (job.args)

        return args.strip()


# MPICH job
class MPICH(MPI):
    key = "mpich"
    name = "MPICH"
    
    def getOptionGroup(parser):
        group = optparse.OptionGroup(parser, "MPICH options")
        group.add_option("", "--mpich-mpdboot", type="string", default="mpdboot",
                         help="mpdboot", metavar="X")
        return group
    getOptionGroup = staticmethod(getOptionGroup)

    def matches(cmd):
	pattern = "mpiexec \[-h or -help or --help\]"
	return MPI.matches(cmd, pattern)
    matches = staticmethod(matches)

    def getInitCmd(self, job):
	global options
	cmd = ""
	if options.mpich_mpdboot and job.hostfile:
            mpdboot = "%s -f %s" % (options.mpich_mpdboot, job.hostfile)
            cmd += "hosts=$(sort %s | uniq | wc -l)\n" % job.hostfile
            cmd += "%s -n $(expr $hosts + 1) || " % mpdboot
            cmd += "%s -n $(expr $hosts + 0)\n" % mpdboot
	return cmd

    def getArgs(self, job):
        global options
        args = ""

        # global args

	if job.hostlist: args += " -hosts %s" %(",".join(job.hostlist))
        if job.hostfile: args += " -machinefile %s" %(job.hostfile)
	#if options.ppn: args += " -perhost %s" %(options.ppn)
        if options.debug and options.debugger == "gdb": args += " -gdb"

	np = options.np
	if not np and job.hostfile:
	    np = 0
	    f = open(job.hostfile)
	    for line in f: np += bool(line.strip())
	    f.close()

        # local args
        if np: args += " -np %s" % np
        #if job.hostlist: args += " -host %s" % job.hostlist[0]
        if job.wdir: args += " -wdir %s" % (job.wdir)
        envlist = ",".join(job.env.keys())
        if envlist: args +=  " -envlist " + envlist
	if options.valgrind: args += " valgrind "
        if job.cmd: args += " %s" % (job.cmd)
        if job.args: args += " %s" % (job.args)

        return args.strip()


# OpenMPI job
class OpenMPI(MPI):
    key = "ompi"
    name = "OpenMPI"
    
    def matches(cmd):
	return MPI.matches(cmd, "(OpenRTE|Open MPI)")
    matches = staticmethod(matches)

    def getArgs(self, job):
        global options
        args = ""

        if options.hostfile: args += " -hostfile %s" % (options.hostfile)
	if options.hostlist: args += " -host %s" % (",".join(options.hostlist))
        if options.np: args += " -np %s" % options.np
	if options.debug: args += " --debug"
	#if options.debugger: args += " --debugger %s" % (options.debugger)
	if options.verbose: args += " -v -d"
        if job.wdir: args += " -wdir %s" % (job.wdir)
        for (k, v) in job.env.items():
            args += " -x %s=\"%s\"" % (k, v)
        if job.cmd: args += " %s" % (job.cmd)
        if job.args: args += " %s" % (job.args)

        return args.strip()


# XT job
class XT(MPI):
    key = "xt"
    mpicmd = "aprun"
    name = "XT"
    def __init__(self):
	pass
         # if options.np: options.pbs += ["-l size=%i" % (options.np)]

    def getCmd(self):
        global options
        cmd = self.mpicmd
        if options.debug and options.debugger == "tv":
            cmd = options.gcmd + " " + cmd + " -a"
        return cmd
            
    def getOptionGroup(parser):
        group = optparse.OptionGroup(parser, "XT options")
        group.add_option("", "--xt-symm-heap", type="string",
                         help="XT_SYMMETRIC_HEAP", metavar="X")
        return group
    getOptionGroup = staticmethod(getOptionGroup)

    def getArgs(self, job):
        global options
        args = ""
        if options.np: args += " -n %s" % (options.np)
        if options.ppn: args += " -N %s" % (options.ppn)
        if options.threads: args += " -d %s" % (options.threads)
        if job.cmd: args += " %s" % (job.cmd)
        if job.args: args += " %s" % (job.args)
        return args.strip()

class Job:
    hostfile = None
    hostlist = None
    input = None
    name = None
    wdir = None
    output = None
    error = None
    env = {}
    cmd = None
    args = None

class GamessJob(Job):
    cmd = "gamess.x"
    envfile = False
    ericfmt = "/dev/null"
    mcppath = "/dev/null"
    extbas = "/dev/null"

    # GAMESS env - tools/getvars.sh
    __env = { "ENVFIL":"envfile",
              "IRCDATA":"irc",  "OUTPUT":"",      "PUNCH":"dat",    "MAKEFP":"efp",
              "INPUT":"F05",
              "AOINTS":"F08",   "MOINTS":"F09",   "DICTNRY":"F10",  "DRTFILE":"F11",
              "CIVECTR":"F12",  "CASINTS":"F13",  "CIINTS":"F14",   "WORK15":"F15",
              "WORK16":"F16",   "CSFSAVE":"F17",  "FOCKDER":"F18",  "WORK19":"F19",
              "DASORT":"F20",   "DFTINTS":"F21",  "DFTGRID":"F22",  "JKFILE":"F23",
              "ORDINT":"F24",   "EFPIND":"F25",   "PCMDATA":"F26",  "PCMINTS":"F27",
              "SVPWRK1":"F26",  "SVPWRK2":"F27",  "MLTPL":"F28",    "MLTPLT":"F29",
              "DAFL30":"F30",   "SOINTX":"F31",   "SOINTY":"F32",   "SOINTZ":"F33",
              "SORESC":"F34",   "GCILIST":"F37",  "HESSIAN":"F38",  "SOCCDAT":"F40",
              "AABB41":"F41",   "BBAA42":"F42",   "BBBB43":"F43",   "MCQD50":"F50",
              "MCQD51":"F51",   "MCQD52":"F52",   "MCQD53":"F53",   "MCQD54":"F54",
              "MCQD55":"F55",   "MCQD56":"F56",   "MCQD57":"F57",   "MCQD58":"F58",
              "MCQD59":"F59",   "MCQD60":"F60",   "MCQD61":"F61",   "MCQD62":"F62",
              "MCQD63":"F63",   "MCQD64":"F64",   "NMRINT1":"F61",  "NMRINT2":"F62",
              "NMRINT3":"F63",  "NMRINT4":"F64",  "NMRINT5":"F65",  "NMRINT6":"F66",
              "DCPHFH2":"F67",  "DCPHF21":"F68",  "ELNUINT":"F67",  "NUNUINT":"F68",
              "GVVPT":"F69",    "NUMOIN":"F69",   "NUMOCAS":"F70",  "NUELMO":"F71",
              "NUELCAS":"F72",  "FCIINF":"F90",   "FCIINT":"F91",   "CCREST":"F70",
              "CCDIIS":"F71",   "CCINTS":"F72",   "CCT1AMP":"F73",  "CCT2AMP":"F74",
              "CCT3AMP":"F75",  "CCVM":"F76",     "CCVE":"F77",     "CCQUADS":"F78",
              "QUADSVO":"F79",  "EOMSTAR":"F80",  "EOMVEC1":"F81",  "EOMVEC2":"F82",
               "EOMHC1":"F83",   "EOMHC2":"F84",   "EOMHHHH":"F85",  "EOMPPPP":"F86",
               "EOMRAMP":"F87",  "EOMRTMP":"F88",  "EOMDG12":"F89",  "MMPP":"F90",
               "MMHPP":"F91",    "MMCIVEC":"F92",  "MMCIVC1":"F93",  "MMCIITR":"F94",
               "EOMVL1":"F95",   "EOMVL2":"F96",   "EOMLVEC":"F97",  "EOMHL1":"F98",
               "EOMHL2":"F99",   "AMPROCC":"F70",  "ITOPNCC":"F71",  "FOCKMTX":"F72",
               "LAMB23":"F73",   "VHHAA":"F74",    "VHHBB":"F75",    "VHHAB":"F76",
               "VMAA":"F77",     "VMBB":"F78",     "VMAB":"F79",     "VMBA":"F80",
               "VHPRAA":"F81",   "VHPRBB":"F82",   "VHPRAB":"F83",   "VHPLAA":"F84",
               "VHPLBB":"F85",   "VHPLAB":"F86",   "VHPLBA":"F87",   "VEAA":"F88",
               "VEBB":"F89",     "VEAB":"F90",     "VEBA":"F91",     "VPPPP":"F92",
               "INTERM1":"F93",  "INTERM2":"F94",  "INTERM3":"F95",  "OLI201":"F201",
               "OLI202":"F202",  "OLI203":"F203",  "OLI204":"F204",  "OLI205":"F205",
               "OLI206":"F206",  "OLI207":"F207",  "OLI208":"F208",  "OLI209":"F209",
               "OLI210":"F210",  "OLI211":"F211",  "OLI212":"F212",  "OLI213":"F213",
               "OLI214":"F214",  "OLI215":"F215",  "OLI216":"F216",  "OLI217":"F217",
               "OLI218":"F218",  "OLI219":"F219",  "OLI220":"F220",  "OLI221":"F221",
               "OLI222":"F222",  "OLI223":"F223",  "OLI224":"F224",  "OLI225":"F225",
               "OLI226":"F226",  "OLI227":"F227",  "OLI228":"F228",  "OLI229":"F229",
               "OLI230":"F230",  "OLI231":"F231",  "OLI232":"F232",  "OLI233":"F233",
               "OLI234":"F234",  "OLI235":"F235",  "OLI236":"F236",  "OLI237":"F237",
               "OLI238":"F238",  "OLI239":"F239",  "LHYPWRK":"F297", "LHYPWK2":"F298",
	      "BONDDPF":"F299", "ITSPACE":"F96","INSTART":"F97", "TRAJECT":"F04",
	      "RIVMAT":"F51", "RIT2A":"F52", "RIT3A":"F53", "RIT2B":"F54", "RIT3B":"F55",
              }

    # GAMESS GDDI env
    __envGDDI = { "IRCDATA":"F04", "OUTPUT":"F06", "PUNCH":"F07" }

    def getOptionGroup(parser):
        group = optparse.OptionGroup(parser, "Gamess options")
        group.add_option("", "--gamess-ericfmt",
                         help="ERICFMT", metavar="X")
        group.add_option("", "--gamess-mcppath",
                         help="MCPPATH", metavar="X")
        group.add_option("", "--gamess-extbas",
                         help="EXTBAS", metavar="X")
        group.add_option("", "--gamess-gddi", action="store_true",
                         help="Gamess GDDI", metavar="X")
        group.add_option("", "--gamess-envfile", action="store_true",
                         help="Gamess ENVFILE", metavar="X")
        group.add_option("", "--gamess-ds", action="store_true",
                         help="GAMESS data server", metavar="X")
        return group
    getOptionGroup = staticmethod(getOptionGroup)

    def __init__(self, input, jobid):
        global options

        if options.gamess:
            exe = os.path.expandvars(options.gamess)
            self.cmd = options.gamess
            if not os.path.isabs(exe) and os.path.basename(exe) != exe:
                self.cmd = os.path.abspath(options.gamess)
        
        if options.gamess_ericfmt: self.ericfmt = options.gamess_ericfmt
        if options.gamess_mcppath: self.mcppath = options.gamess_mcppath
        if options.gamess_extbas: self.extbas = options.gamess_extbas
        if options.gamess_gddi: self.gddi = True
        if options.gamess_envfile: self.envfile = True

        self.input = input
        self.name = os.path.basename(self.input.strip())
        # strip off suffix
        if self.name.lower().endswith(".inp"):
            self.name = self.name[:len(self.name)-4]

        # working directory
        self.wdir = self.name
        if jobid: self.wdir = self.wdir + ".%s" % (jobid)
        if options.wdir: self.wdir = os.path.join(options.wdir, self.wdir)

        # output
        if not options.interactive:
            if options.output: self.output = options.output
            else: self.output = os.path.join(os.getcwd(), self.name + ".log")
            if options.error: self.error = options.error
            else: self.error = os.path.join(os.getcwd(), self.name + ".log")

        if self.envfile: self.env = {"ENVFIL":self.getEnv("ENVFIL", self)}
        else: self.env = self.getAllEnv()
        self.env.update(options.env)
                                    
        self.cmd = os.path.expandvars(self.cmd)
        self.args = ""
        
    # return GAMESS env
    def getAllEnv(self):
        env = {}
        for (k,v) in self.__env.items():
            if v: env[k] = self.getEnv(k)
        if self.ericfmt: env["ERICFMT"] = os.path.expandvars(self.ericfmt)
        if self.mcppath: env["MCPPATH"] = os.path.expandvars(self.mcppath)
        if self.extbas: env["EXTBAS"] = os.path.expandvars(self.extbas)
        return env

    def getEnv(self, key):
        value = self.__env.get(key)
        if value: return os.path.join(self.wdir, "%s.%s" % (self.name, value))
        else: return None

    def isGddi(self):
        if self.gddi: return True
        else:
            pattern = re.compile("^ \$gddi\s+", re.IGNORECASE)
            fh = open(self.input, "r")
            for line in fh:
                if pattern.match(line):
                    fh.close()
                    return True
            fh.close()
            return False

    # write working dir
    def getProlog(self, escape="\\"):
        global options
        wdir = self.wdir
        input = self.getEnv("INPUT")
        prolog = ""

        #prolog += "rm -fr %s/%s.*\n" % (self.wdir, self.name)
        prolog += "rm -fr %s/\n" % (self.wdir)
        prolog += "mkdir -p %s\n" % (self.wdir)
        prolog += "\n"


        if self.envfile:
            prolog += "\n"
            prolog += "cat >> %s << EOF\n" % (self.env["ENVFIL"])
            for (k, v) in self.getAllEnv().items():
                prolog += "%s=%s\n" % (k, v)
            prolog += "EOF\n"

        return prolog

    def files(self):

	class input_tuple:
	    name = ""
	    string = ""

        f = open(self.input, "r")
	inp = input_tuple
	inp.name = self.getEnv("INPUT")
        inp.string = f.read()
        # for line in f:
	#     if line.strip() and line.strip()[0] == "!": continue
	#     inp.string += "%s" % line #(line[:-1].replace("'", ""))
        f.close()
	return [ inp ]

    # write ending of the script
    def getEpilog(self, escape="\\"):
        global options
        script = ""
        script += "rm -f %s/*.F*\n" % (self.wdir)
        script += "rm -f %s/*.h5\n" % (self.wdir)
        if not options.keep: script += "rm -fr %s/\n" % (self.wdir)
        return script


def warning(message):
    print "Warning: %s" % (message)

    
def error(message):
    print "Error: %s" % (message)
    sys.exit(1)


def opentemp(prefix=None, suffix=None):
    (fd, fn) = tempfile.mkstemp(prefix=prefix, suffix=suffix)
    os.close(fd)
    return open(fn, "w")


if __name__ == "__main__":
    main()
