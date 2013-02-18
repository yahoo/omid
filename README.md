Omid
=====

The Omid project provides transactional support for key-value stores using Snapshot Isolation. Omid stands for Optimistically transactional Management in Datasources. At this stage of the project, HBase is the only supported data-store.

If you have any question, please take a look to the [Wiki](https://github.com/yahoo/omid/wiki) or contact us at omid-project@googlegroups.com or read [the online archives](https://groups.google.com/forum/?fromgroups=#!forum/omid-project)

Use Cases
---------

Add UCs.

Basic Architecture
------------------

The main component of Omid is a server called the Status Oracle (TSO.) The TSO contains all the information needed to manage transactions. Applications requiring transactional support in key-value stores need to use the API provided by special components provided by Omid called Transactional Clients (TCs). TCs are in charge of connecting to the TSO and perform the required operations in data-stores. The TSO replicates transactional information to the TCs which just contact the TSO when they want to start a transaction or commit it.

The TSO uses BookKeeper as a Write-Ahead Log where it dumps all its state. In case of crash failures it is possible to restart the TSO without losing any commit information.

The core architecture of the software is described in more detail in the [Technical Details](https://github.com/yahoo/omid/wiki/Technical-Details) section of the Wiki.

Compilation
-----------

Omid uses Maven for its build system. We are using a temporary repository for Zookeeper and Bookkeeper packages to ease the installation procedure.

To compile Omid:

    $ tar jxvf omid-1.0-SNAPSHOT.tar.bz2
    $ cd omid-1.0-SNAPSHOT
    $ mvn install

Tests should run cleanly.

Set-Up
------

You need to run four components before running the transactional client in your applications: Bookkeeper, Zookeeper, Omid TSO and HBase. These are their dependencies:

1. Bookkeeper is needed by the TSO
2. Zookeeper is needed by Bookkeeper and HBase. 
3. The TSO is needed by HBase. 

Hence, the order of starting should be:

1. Zookeeper
2. Bookkeeper
3. TSO
4. Hbase

### Zookeeper & Bookkeepergit
Omid doesn't use anything special in Zookeeper or Bookkeeper, so you can use any install for these. However, if you are running this anywhere but localhost, you need to update the setting for HBase and TSO. See the HBase docs for changing the Zookeeper quorum. For TSO, you need to modify bin/omid.sh.

For simplicity we've included a utility script which starts Zookeeper and Bookkeeper. Run:

    $ bin/omid.sh bktest

### TSO
To start the TSO, run:
   
    $ bin/omid.sh tso

### Benchmark
To benchmark the TSO alone, run:

    $ bin/omid.sh tsobench

### HBase
We've included a utility script to start a HBase cluster on your local machine. Run:

    $ bin/omid.sh tran-hbase

For running in a cluster

API Description
---------------

The public API is in these classes:

    src/main/java/com/yahoo/omid/client/TransactionalTable.java
    src/main/java/com/yahoo/omid/client/TransactionState.java
    src/main/java/com/yahoo/omid/client/TransactionManager.java

For an example of usage, take a look to this class:

    src/test/java/com/yahoo/omid/TestBasicTransaction.java

Logging 
-------
The logging preferences can be adjusted in src/main/resources/log4j.properties.

Acknowledgement
-------
This project has been partially supported by the EU Comission through the Cumulo Nimbo project (FP7-257993).