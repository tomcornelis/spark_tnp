# spark-tnp
Tag and probe analysis using Apache Spark.

## Interactive notebook

There are example notebooks in the [notebooks](notebooks) directory demonstrating the use of these tools interactively.
A good starting point is to follow the instructions in [MuonTnP.ipynb](notebooks/MuonTnP.ipynb).
These notebooks use [swan.cern.ch](swan.cern.ch) to connect to the Apache Spark clusters at CERN.

More details on CERN's Apache Spark can be found [here](https://hadoop-user-guide.web.cern.ch/hadoop-user-guide/spark/Using_Spark_on_Hadoop.html).

## Command line setup

There are a couple of ways you can run. Either connect to the edge node or directly on lxplus.
The jobs are run on spark clusters and the data is read from an hdfs cluster.
The default (and preferred) way is to use the `analytix` spark and hdfs cluster.

### Edge node

Connect to the hadoop edge node (from within the CERN network):

```bash
ssh it-hadoop-client
```

Setup the environment:

```bash
source /cvmfs/sft.cern.ch/lcg/views/LCG_96bpython3/x86_64-centos7-gcc8-opt/setup.sh
source hadoop-setconf.sh analytix
```

### LXPLUS

Connect to LXPLUS:

```bash
ssh lxplus.cern.ch
```

Setup the environment:

```bash
source env.sh
```

## Tag-and-probe steps

The tag-and-probe process is broken down into several parts:

1. Creation of the flat ROOT tag-and-probe trees (not shown here)
2. Conversion of the ROOT TTree into the parquet data format
3. Reduce the data into binned histograms with spark
4. Fit the resulting histograms
5. Extraction of efficiencies and scale factors

These steps are controlled with the [tnp_fitter.py](tnp_fiter.py) script.
For help with the script run:
```bash
./tnp_fitter.py -h
```

### Conversion to parquet

The conversion to parquet vastly speeds up the later steps.
We will use [laurelin](https://github.com/spark-root/laurelin) to
read the root files and then write them in the parquet data format.
There are two possible approaches: using `k8s` and using `analytix`.

Conversion with `k8s` currently only works if you are using [swan.cern.ch](swan.cern.ch).
Use the [RootToParquet](notebooks/RootToParquet.ipynb) notebook as a guide.
The output should be writting to `analytix`.

Conversion with `analytix` requires you to first copy your root files
to `hdfs://analytix`. There is an issue with reading root files from `eos`
on `analytix` that needs to be understood.

```bash
hdfs dfs -cp root://eoscms.cern.ch//eos/cms/store/[path-to-files]/*.root hdfs://analytix/[path-to-out-dir]
```

Once copied, you can use:

```bash
./tnp_fitter.py convert [particle] [resonance] [era]
```

**Note:** this will currently raise a `NotImplemented` exception.
You can look at [converter.py](converter.py) for how to run things
until it is incorporated.

### Flatten histograms with spark

This step uses the converted parquet data format to efficiently aggregate
the efficiency data into binned histograms.

```bash
./tnp_fitter.py flatten -h
```

For example, to flatten all histograms for the Run2017 Legacy muon scalefactors from Z:

```bash
./tnp_fitter.py flatten muon Z Run2017_UL
```

You can optionally filter the efficiencies and shifts you flatten with the `--numerator`,
`--denominator`, and `--shiftType` arguments. Thus, to only flatten the nominal histograms do:
```bash
./tnp_fitter.py flatten muon Z Run2017_UL --shiftType Nominal
```

**Note:** running this on lxplus will give the following warnings:

>WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable  
>WARN DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.  
>WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.  
>WARN YarnSchedulerBackend$YarnSchedulerEndpoint: Attempted to request executors before the AM has registered!  
>WARN TableMapping: /etc/hadoop/conf/topology.table.file cannot be read.  
>java.io.FileNotFoundException: /etc/hadoop/conf/topology.table.file (No such file or directory)  
>...  
>WARN TableMapping: Failed to read topology table. /default-rack will be used for all nodes.  

and

>WARN Utils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.debug.maxToStringFields' in SparkEnv.conf.  

which can be safely ignored.

### Fit histograms

Histogram fitting uses local running or condor.

To run locally (with 16 threads):
```bash
./tnp_fitter.py fit muon Z Run2017_UL -j 16
```

To submit to condor:
```bash
./tnp_fitter.py fit muon Z Run2017_UL --condor
condor_submit condor.sub
```

The histograms which are fit can be controlled with optional filters.
See documentation with:
```bash
./tnp_fitter.py fit -h
```

### Extract scale factors

TODO
