# Trace-Repair over Apache Hadoop

The `trace-repair` branch implements our Trace-Repair algorithm over a 9-6 scheme on top of Hadoop Distributed File
System (HDFS). Further information regarding this method can be found in
this [paper](https://arxiv.org/pdf/2007.15253.pdf).

## Execution

To run the trace-repair method locally, first clone the repository and checkout the `trace-repair` branch:

```bash
git clone https://github.com/kinda-raffy/HDFS_RSC_Repair.git
cd HDFS_RSC_Repair
git checkout trace-repair
```

Then, launch the execution environment in docker:

```bash
./start-build-env.sh
```

Alternatively, follow the instructions in this [document](./BUILDING.txt) to set up the execution environment locally (
without docker), although the docker method is preferred.

Once the execution environment is ready, run the following command to execute the trace-repair method:

```bash
benchmarkTraceRepair
```

This command will compile and run a holistic test case that will benchmark the trace-repair method. This test case will
create a 6MB file populated with random data, then it will simulate a datanode failure which will prompt Hadoop to
schedule a reconstruction job using our trace repair method. This method will be benchmarked, and the test case will
intermittently read the contents of the recovery node to verify the contents are correct.

Note that this method may take a while to run, as it will require to download and compile Hadoop from source. The
results of the benchmark will be printed to the console at the end.