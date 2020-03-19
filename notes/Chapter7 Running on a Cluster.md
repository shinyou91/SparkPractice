# Chapter7 Running on a Cluster

## Introduction

Benefits of writing applications on Spark
1. the ability to scale computation by adding more machines and running in cluster mode
2. users can rapidly prototype applications on smaller datasets locally, then run unmodified code on even very large clusters

- Spark can run on a variety of cluster managers in both on-premise and cloud deployments.


## Spark Runtime Architecture

In distributed mode, Spark uses a *master/slave* architecture with
- one central coordinator —— **driver**
	- communicates with a potentially large number of distributed workers
	- runs its own Java process
- many distributed workers —— **executors**
	- a separate Java process
- a driver and its executors are together termed a Spark **application**
	- a Spark application is launched on a set of machines using an external service called a **cluster manager**
		- Spark is packaged with a built-in cluster manager called the Standalone cluster manager

<br>

### The Driver

- the driver is the process 
	- where the main() method of your program runs.
	- running the code that creates a SparkContext, creates RDDs, and performs transformations and actions

- when you launch a spark shell, you've created a driver program

- when the driver runs, it performs 2 duties :
	1. Converting a user program into tasks
		- all Spark programs follow the same structure :
			1. create RDDs from some inputs
			2. derive new RDDs from those using transformations
			3. perform actions to collect or save data
		- a Spark program inplicitly creates a logical *directed acyclic graph* (DAG) of operations.
			- when the driver runs, it converts the logical graph into a physical execution plan called *tasks*
		- tasks are the smallest unit of work in Spark
			- a typical user program can launch hundreds or thousands of individual tasks
	2. Scheduling tasks on executors
		- each executor represents a process capable of running tasks and storing RDD data
			- when executors are started they register themselves with the driver
			- the driver will look at the current set of executors and try to schedule each tasks in an appropriate location
				- based on data placement and the location of cached data
		- information about the running application : `localhost:4040`

<br>

### Executors

- Executors are launched once at the beginning of a Spark application and typically run for the entire lifetime of an application
	- though Spark applications can continue if executors fail

- 2 roles of Executors
	1. run the tasks that make up the application and return results to the driver
	2. provide in-memory storage for RDDs that are cached by user programs, through a service called the **Block Manager** that lives within each executor.
		- because RDDs are cached directly inside of executors, tasks can run alongside the cached data

> in local mode, the Spark driver runs along with an executor in the same Java process

<br>

### Cluster Manager

- Spark depends on a cluster manager to launch executors and, in certain case, to launch the driver

- the cluster manager is a pluggable component in Spark
	- this allows Spark to run on top of different external managers, as well as its built-in Standalone cluster manager

> NOTE THAT
>> the terms **driver** and **executor** are used to describe the process that execute each Spark application

>> the terms **master** and **worker** are used to describe the centralized and distributed portion of the cluster manager.

<br>

### Launching a Program

- **spark-submit** 
	- can connect to different cluster managers
	- can control how many resources your application gets

<br>

### Summary

steps that occur when you run a Spark application on a cluster : 
1. user submits an application using **spark-submit**
2. spark-submit launches the driver program and invokes the main() method specified by the user
3. the driver program contacts the cluster manager to ask for resources to launch executors
4. the cluster manager launches executors on behalf of the driver program
5. the driver process runs through the user application
	- based on the RDD actions and transformations in the program, thr driver sends work to executors in the form of tasks
6. tasks are run on executor processes to compute and save results
7. if the driver's main() method exits or it calls SparkContext.stop(), it will terminate the executors and relaese sources from the cluster manager.


<br>

## Deploying Applications with spark-submit

- run locally
	- when spark-submit is called with nothing but the name of a script or JAR

> submit a python application
```bash
bin/spark-submit my_script.py
```

- submit to a Spark Standalone cluster
	- provide extra flags with the address of a Standalone cluster and a specofic size of each executor process we'd like to launch

> submit an application with extra arguments
```bash
bin/spark-submit --master spark://host:7077 --executor-memory 10g my_script.py
```
--master : a cluster URL to connect to<br>
spark:// URL : means a cluster using Spark's Standalone mode

<br>

> possible values for the --maste r flag in spark-submit

value | explanation
 :-: | :-:
spark://host:port | connect to a Spark Standalone cluster at the specified port. <br>by default uses port **7077**
mesos://host:port | connect to a Mesos cluster master at the specified port. <br> by default listen on port **5050**
yarn | connect to a Yarn cluster. <br> when running on YARN you'll need to set the HADOOP_CONF_DIR environment variable to point the locationof Hadoop configuration directory
local | run in local mode with a single core
local[N] | run in local mode with N cores.
local[*] | run in local mode and use as many cores as the machine has


other options
1. scheduling information
2. runtime dependencies of your application

> general format for spark-submit
```bash
bin/spark-submit [options] <app jar | python file> [app options]
```

[options] : a list of flags for spark-submit. you can enumerate all possible flags by running
```bash
spark-submit --help
```

<app jar | python file> : refers to the JAR or Python script containing the entry point into your application.

[app options] : options that will be parsed onto your application

> common flags for spark-submit

flag | explanatinos
 :-: | :-:
--master | indicates the cluster manager to connect to.
--deploy-mode | whether to launch the driver program locally("client") or one of the worker machines inside the cluster("cluster") <br> the default is client mode.
--class | (if Java or Scala) the "main" class of your application
--name | a human-readable name for  the application. this will be displayed in Spark's web UI
--jars |  a list of JAR files to upload and place on the classpath of your application.
--files | a list of files to be placed in the working directory of your application. this can be used for data files that you want to distribute to each node.
--py-files | a list of files to be added to the PYTHONPATH of your application. <br> this can contain *.py, .egg* or *.zip* files
--executor-memory | the amount of memory to use for executors in bytes.
--driver-memory | the amount of memory to use for the driver program.


spark-submit also allows setting arbitrary SparkConf configuration options 
- using **--conf prop=value** flag
- providing a properties file through **--properties-file** that contains key/value pairs

> using spark-submit with various options
```bash
# submitting a Python application in YARN client mode

$ export HADOOP_CONF_DIR=/opt/hadoop/conf
$ .bin/spark-submit \
  --master yarn \
  --py-files somelib-1.2.egg, otherlib-4.4.zip, other-file.py \
  --deploy-mode client \
  --name "Example Program" \
  --quene exampleQueue \
  --num-executors 40 \
  --executor-memory 10g \
  my_script.py "options" "to your application" "go here"
```

<br>

## Packaging Your Code and Dependencies

for python, there are a few ways to install third-party libraries.
1. since pyspark uses the existing python installation on worker machines, you can install ddependency libraries directly on the cluster machines
	- using standard python pachage managers (such as *pip* or *easy_install*)
	- via a manual installation into the *site-packages/* directory of your python installation
2. you can submit individual libraries using the **--py-files** argument to spark-submit
3. (for Java and Scala) it is also possible to submit individual JAR files using the *--jar* flag to spark-submit
	- when you submit an application to Spark, it must ship with its entire *transitive dependency graph* to the cluster
	- common practice to rely on a build tool to produce a single large JAR containing the entire transive dependency graphof an application
		- called **uber JAR** or **assembly JAR**

> when you are building an application, you should never include Spark itself in the list of submitted dependencies.

the most popular build tools :
- Maven : often for Java
- sbt : often for scala
Either tool can be used with either language


### A Scala Spark Application Built with sbt

1. At the root of your project you create a build file called **build.sbt**<br>
	- sbt build files are written in a configuration language where you assign values to specific keys in order to define the build for your project
2. your source code is expected to live in *src/main/scala*

> build.sbt file for a Spark application built with sbt 0.13
```bash
// import some functionality from an sbt build plug-in that supports creating project assembly JARs
import AssenblyKeys._

name := "simple project"

version := "1.0"

organization := "com.databricks"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq{
	// spark dependency
	"org.apache.spark" % "spark-core_2.10" % "1.2.0" % "provided", 
	// third-party libraries
	"net.sf.jopt-simple" % "jopt-simple" % "4.3",
	"joda-time" % "joda-time" % "2.0"
}

// this statement includes the assembly plug-in capabilities
assemblySettings

// configure JAR used with the assembly plug-in
jarName in assembly := "my-project-assembly.jar"

// a special option tp exclude Scala itself from our assembly JAR, since Spark already bundles scala
assemblyOption in assembly :=
	(assemblyOption in assembly).value.copy(includeScala = false)
```

to enable this plug-in, we also have to include a small file in a *project/* directory that lists the dependency on the plug-in

> adding the assembly plug-in to an sbt project build
```bash
# display contents of project/assembly.sbt
$ cat project/assembly.sbt
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")
```

> create a fully assembled Spark application JAR
```bash
$ sbt assemly

# in the target directory, we'll see an assembly JAR
$ ls target/scala-2.10/my-project-assembly.jar

# listing the assembly JAR will reveal classes from dependency libraries
$ jar tf targer/scala-2.10/my-project-assembly.jar
...
joptsimple/HelpFormatter.class
...
org/joda/time/tz/UTCProvider.class
...

# an assembly JAR can be passed directly to spark-submit
$ /path/to/spark/bin/spark-submit --master local ...
target/scala-2.10/my-project-assembly.jar
```

<br>

### Dependency Conflicts

one occationally disruptive issue : <br>
dealing with dependency conflicts in cases where a user application and Spark itself both depend on the same library
- NoSuchMethodError, ClassNotFoundException or some other JVM exception related to class
- 2 solutions
	1. modify your application to depend on the same version of the third-party library that Spark does
	2. modify the packaging of your application using a procedure called "shading"
		- shading allows you to make a second copy of the conflicting package under a different namespace <br>and rewrites your application's code to use renamed version
		- quite effective at resolving runtime dependency conflicts

<br>

## Scheduling Within and Between Spark Applications

for scheduling in multitenant clusters, Spark primarily relies in the cluster manager to share resources between Spark applications.

one special case : *long live* applications that are never intended to terminate
<br> spark provides a finer-grained mechanism through configurable intra-application scheduling policies.


### Cluster Manager

spark can run
- by itself on a set of machines
- over two popular cluster managers : Hadoop YARN and Apache Mesos

#### Standalone Cluster Manager

when you submit an application, you can choose how much memory its executors will use,
<br> as well as the total number of cores across all executors.

#### Launching the Standalone Cluster Manager

we can start the Standalone Cluster Manager
	- by starting a master and workers by hand
	- by using launch scripts in Spark's *sbin* directory
		- the simplest option
		- require SSH access between your machines
		- available only on Mac OX and Linux

- steps to use the cluster launch scripts :
1. copy a compiled version of Spark to the same location on all your machines
2. set up password-less SSH access from your master machine to the others.
	- requires 
		- having the same user account in all the machines
		- creating a private SSH key for it on the master via `ssh-keygen`
		- adding this key to the *.ssh/authorized_keys* file of all the workers

> follow the command
>> on master : run ssh-keygen accepting default options 
```bash
$ ssh-keygen -t dsa
Enter file in which to save the key (/home/you/.ssh/id_dsa): [ENTER]
Enter passphrase (empty for no passphrase) : [EMPTY]
Enter same passphrase again : [EMPTY]
```
>> on workers : copy ~/.ssh/id_dsa.pub from your master to worker, then use
```bash
$ cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
$ chmod 644 ~/.ssh/authorized_keys
```

3. edit the *conf/slaves* file on your master and fill in the worker's hostnames
4. run `sbin/start-all.sh` on your master
	- get no promts for a password
	- the cluster manager's web UI appear at `http://masternode:8080` and show all your workers
5. run `bin/stop-all.sh` on your master node to stop the cluster

<br>

- steps to start the master and workers by hand using the *spark-class* script in Spark's *bin/* directory :
1. on master, type
```bash
$ bin/spark-class org.apache.spark.deploy.master.Master
```
2. on workers
```bash
$ bin/spark-class org.apache.spark.deploy.worker.Worker spark://masternode:7077
```

<br>

#### Submitting applications

to submit an application to the Standalone cluster manager, pass `spark://masternode:7070` as the master argument to spark-submit
```bash
$ spark-submit --master spark://masternode:7077 yourapp
```

> note that the hostnames and port used during submission must **exactly match** the URL present in the UI

to check your application or shell is running, look at the cluster manager's web UI `http://masternode:8080` 
<br> and make sure that : 
1. your application is connected
2. it is listed as having more than 0 cores and memory

> a common pitfall that might prevent your application from running :
>> requesting more memory for executors than is available in the cluster


<br>
 the Standalone cluster manager supports 2 *deploy modes* for where the driver program of your application runs
1. client mode(default)
	- the driver runs on the machine where you executed spark-submit
	- you can directly see the output of your driver program, or send input to it
	- it requires the machine from which your application was submitted to have fast connectivity to the workers <br>and to stay available for the duration of your application

2. cluster mode
	- the driver is launched within the Standalone cluster, as another proecss on one of the worker nodes, and then it connects back to request executors.
	- you can close your laptop while the application is running
	- switch to cluster mode by passing `--deploy-mode` to spark-submit

#### Configuring resource usage

In the standalone cluster manager, resource allocation is controlled by 2 settings :
1.**Executor Memory** : using `--executor-memory` argument to spark-submit
	- each application will have at mode one executor in each worker
		- this setting controls how much of that worker's memory the application will claim
	- default 1GB

2. **The maximum number of total cores** used across all executors for an application(default unlimited)
	- set this value 
		- through the `--total-executor-cores` argument to spark-submit
		- or by configuring `spark.core.max` in Spark configuration file

- see the current resource allocation in the Standalone cluster manager's web UI `http://masternode:8080`

- the Standalone cluster manager works by spreading out each application across the maximum number of executors by default
	- so that  we can give applications a chance to achieve data locality for distributed filesystems running on the same machines

- you can ask Spark to consolidate executors on as few nodes as possible by 
<br>setting the config property `spark.deploy.spreadOut` to false in *conf/spark-defaults.conf*

#### High availability

the Standalone mode will support the failure of worker nodes.


<br>

## Which Cluster Manager to Use?

- start with a standalone cluster if this is a new deployment
- if you'd like to run Spark alongside other applications or to use richer resource scheduling capabilities, both YARN and Mesos provide these features
- One advantage of Mesos over both YARN and Standalone mode is its finegrained sharing option, which lets interactive applications such as the Spark shell
<br>scale down their CPU allocation between commands
- in all cases, it is best to run Spark on the same nodes as HDFS for fast access to storage















