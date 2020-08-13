

# spark-tnp
Tag and probe analysis using Apache Spark.

## Step-by-step instruction (as done for the leptonMva scale factors)

This package is quite complicated, we therefore list here everything step by step


### Before you begin

- Request access to the cluster in the help document found [here](https://hadoop-user-guide.web.cern.ch/hadoop-user-guide/getstart/access.html).
- Log into [https://swan.cern.ch](https://swan.cern.ch) with software stack *96 python 3* and spark cluster *cloud containers (8KS)*
- Click on "download project from git" and use [https://github.com/tomcornelis/spark\_tnp](https://github.com/tomcornelis/spark_tnp) (or your own fork)
- Once logged in, you will get a /eos/user/${USER:0:1}/$USER/SWAN\_projects directory in your eos area, in which you can find the spar\_tnp git repository.
  At the top bar in [https://swan.cern.ch](https://swan.cern.ch) you can choose to start a new terminal if needed, but it is easier to simple access the
  git repository through good old lxplus, especially if you want to have your usual shell environment.

### ROOT to PARQUET

- Connect to a hadoop edge node (from lxplus):
```
  ssh it-hadoop-client
  kinit
  source /cvmfs/sft.cern.ch/lcg/views/LCG_97python3/x86_64-centos7-gcc8-opt/setup.sh
  source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-setconf.sh analytix
```
- Update the paths (if needed) in the converter.py script and run it
```
  ./converter.py
```
- The above step might fail a few times randomly. Comment out the already finished ones in the script and repeat.
- In order to avoid disk space issues the ROOT files are copied by the script one by one to the analytix cluster and deleted immediately after.


### ROOT to PARQUET (alternative way)

- In SWAN, go to the following notebook: spark\_tnp/notebooks/RootToParquet.ipynb
- Run the first cell by clicking on "Run" once
- Start the Spark cluster connection by clicking on the star (it seems default settings are those who are mentioned in the notebook documentation)
- Run cell 2
- Check if the paths to the filenames in cell 3 are still correct
- Run the next cells


### Flattening
- Make sure the paths in the parquet fields in data/registry\_leptonMva.json are correct
- Run
```
  ./tnp_fitter.py flatten muon generalTracks Z Run2016 configs/leptonMva.json
  ./tnp_fitter.py flatten muon generalTracks Z Run2017 configs/leptonMva.json
  ./tnp_fitter.py flatten muon generalTracks Z Run2018 configs/leptonMva.json
```

### Fitting
- Run
```
  ./tnp_fitter.py fit muon generalTracks Z Run2016 configs/leptonMva.json -j 16
  ./tnp_fitter.py fit muon generalTracks Z Run2017 configs/leptonMva.json -j 16
  ./tnp_fitter.py fit muon generalTracks Z Run2018 configs/leptonMva.json -j 16
```
This will take a very long time, and you need to keep your lxplus connection open. It will probably be closed anyway, so then you can proceed in a new connection
with the same commands and adding the --recover argument which will run the unfinished jobs.
- Check the baseDir, which can be given as an additional argument or is redirecting to its default value specified in the tnp\_fitter.py, that's the place where the fits are ending up.
The png files there could be shown on a website.


### Plotting
- Run
```
  ./tnp_fitter.py prepare muon generalTracks Z Run2016 configs/leptonMva.json
  ./tnp_fitter.py prepare muon generalTracks Z Run2017 configs/leptonMva.json
  ./tnp_fitter.py prepare muon generalTracks Z Run2018 configs/leptonMva.json
```



## Before you begin
This package uses Apache Spark clusters.
More details on CERN's Apache Spark can be found [here](https://hadoop-user-guide.web.cern.ch/hadoop-user-guide/spark/Using_Spark_on_Hadoop.html).

**Important:** If you want to use the CERN analytix cluster (which is much faster to startup than the k8s cluster),
you need to request access to the cluster in the help document found [here](https://hadoop-user-guide.web.cern.ch/hadoop-user-guide/getstart/access.html).

## Quick start

The following will produce a set of example efficiencies (assuming you can run on analytix):

```bash
git clone https://github.com/dntaylor/spark_tnp.git
cd spark_tnp
source env.sh
kinit
./tnp_fitter.py flatten muon generalTracks Z Run2018_UL configs/muon_example.json --baseDir ./example
./tnp_fitter.py fit muon generalTracks Z Run2018_UL configs/muon_example.json --baseDir ./example
./tnp_fitter.py prepare muon generalTracks Z Run2018_UL configs/muon_example.json --baseDir ./example
```

## Interactive notebook

There are example notebooks in the [notebooks](notebooks) directory demonstrating the use of these tools interactively.
A good starting point is to follow the instructions in [MuonTnP.ipynb](notebooks/MuonTnP.ipynb).
These notebooks use [https://swan.cern.ch](https://swan.cern.ch) to connect to the Apache Spark clusters at CERN.

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
kinit
source /cvmfs/sft.cern.ch/lcg/views/LCG_97python3/x86_64-centos7-gcc8-opt/setup.sh
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

**Note**: Do not forget to make sure you have a valid kerberos token with:
```bash
kinit
```

### Optional

Install `tqdm` packaged for a nice progressbar.

```bash
pip install --user tqdm
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

The most important argument to pass is the configuration file
that controls what kind of fits are produced.
See detailed documentation in the [configs](configs) directory.

New tag-and-probe datasets will need to be registered in the [data](data) directory.

### Conversion to parquet

The conversion to parquet vastly speeds up the later steps.
We will use [laurelin](https://github.com/spark-root/laurelin) to
read the root files and then write them in the parquet data format.
There are two possible approaches: using `k8s` and using `analytix`.

Conversion with `k8s` currently only works if you are using [https://swan.cern.ch](https://swan.cern.ch).
Use the [RootToParquet](notebooks/RootToParquet.ipynb) notebook as a guide.
The output should be written to `analytix`.

Conversion with `analytix` requires you to first copy your root files
to `hdfs://analytix`. There is an issue with reading root files from `eos`
on `analytix` that needs to be understood.
The following should be executed when you are connected to the edge node.

```bash
hdfs dfs -cp root://eoscms.cern.ch//eos/cms/store/[path-to-files]/*.root hdfs://analytix/[path-to-out-dir]
```

Additionally, you will need to download the `jar` files to add
to the spark executors:

```bash
bash setup.sh
```

Once copied, you can use:

```bash
./tnp_fitter.py convert [particle] [probe] [resonance] [era]
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
./tnp_fitter.py flatten muon generalTracks Z Run2017_UL configs/muon_pog_official_run2_Z_2017.json
```

You can optionally filter the efficiencies and shifts you flatten with the `--numerator`,
`--denominator`, and `--shiftType` arguments. Thus, to only flatten the nominal histograms do:
```bash
./tnp_fitter.py flatten muon generalTracks Z Run2017_UL configs/muon_pog_official_run2_Z_2017.json --shiftType Nominal
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

**Note:** the first time you run a fit it will compile the Root classes. Don't use `-j` option the first time you run fits.
It will try to compile the modules multiple times and throw errors. Instead, use single core to run one fit, then ctrl-c and use the `-j` option
(it won't compile again).

To run locally (with 16 threads):
```bash
./tnp_fitter.py fit muon generalTracks Z Run2017_UL configs/muon_pog_official_run2_Z_2017.json -j 16
```

To submit to condor:
```bash
./tnp_fitter.py fit muon generalTracks Z Run2017_UL configs/muon_pog_official_run2_Z_2017.json --condor
condor_submit condor.sub
```

The histograms which are fit can be controlled with optional filters.
See documentation with:
```bash
./tnp_fitter.py fit -h
```

There is a simple automatic recovery processing that can be run
(in case of condor failures).
More advanced options (such as using statistical tests to evaluate the GOF)
are still being implemented.
```bash
./tnp_fitter.py fit muon generalTracks Z Run2017_UL configs/muon_pog_official_run2_Z_2017.json -j 16 --recover
```

### Extract scale factors

Plots and scalefactors can the be extracted with:
```bash
./tnp_fitter.py prepare muon generalTracks Z Run2017_UL configs/muon_pog_official_run2_Z_2017.json --condor
```

**Note:** this is still a WIP.

## Utilities

### Pileup
The [make_pileup.py](make_pileup.py) script produced the pileup distribution in MC.
This part requires a CMSSW environment sourced.

To make the data pileup, copy the latest PileupHistogram from:
```bash
/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/{COLLISION_ERA}/{ENERGY}/PileUp/PileupHistogram-{...}.root
```
You should grab the `69200ub` version. If you wish to explore systematic uncertainties
in the choice of the minbias cross section, use the up (`66000ub`) and down (`72400ub`) histograms.

Alternatively, you can make it yourself with (e.g. Run2017):
```bash
lumimask=/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/ReReco/Cert_294927-306462_13TeV_EOY2017ReReco_Collisions17_JSON.txt
pileupjson=/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/PileUp/pileup_latest.txt
xsec=69200
maxBins=100
pileupCalc.py -i $lumimask --inputLumiJSON $pileupjson --calcMode true  --minBiasXsec $xsec --maxPileupBin $maxBins --numPileupBins $maxBins pileup/data/Run2017.root
```
