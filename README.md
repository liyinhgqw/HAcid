# HAcid
HAcid enables multi-row transactions in HBase. This is a decentralized transaction manager that runs on the client side through a jar library, using special HBase tables for transactional metadata. The isolation level for transactions is by default Snapshot Isolation, but Serializable Snapshot Isolation is also supported, although it is in early development stage.

## Installing

* Download the library from `dist/HAcid.jar` in this repository.
* Include the library in your project.
* Make sure you have a working HBase installation, and the path to its configuration's `hbase-site.xml`.
* Use the library's API to make and submit transactions.

Nothing else is required. No specialized server process is employed. Just plug and play.

## Usage

```
import fi.aalto.hacid.HAcidClient;
import fi.aalto.hacid.HAcidGet;
import fi.aalto.hacid.HAcidPut;
import fi.aalto.hacid.HAcidTable;
import fi.aalto.hacid.HAcidTxn;
```

**Initialize the HAcid client.** Point to your HBase configuration.
```
Configuration conf = HBaseConfiguration.create();
HAcidClient client = new HAcidClient(conf);
```

**Refer to your HBase table using HAcid's wrapper class** (this enables your table for multi-row transactions)
```
HAcidTable table = new HAcidTable(conf, "mytable");
```

**Start a transaction**
```
HAcidTxn txn = new HAcidTxn(client);
```

**Insert read/write operations in the transaction**
```
HAcidGet g = new HAcidGet(table, Bytes.toBytes("row1"));
g.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("qual1"));
Result r = txn.get(g);

HAcidPut p = new HAcidPut(table, Bytes.toBytes("row1"));
p.add(Bytes.toBytes("fam1"), Bytes.toBytes("qual1"), Bytes.toBytes("value"));
txn.put(p);
```

**Send a commit request to the transaction, and get the outcome**
```
boolean outcome = txn.commit(); // true is "committed", false is "aborted"
```

## Features

* ACID transactions with Snapshot Isolation
* Unbounded number of operations per transaction
* Unbounded number of tables and rows that a single transaction can span
* Client-side transaction management algorithms (no server-side changes)

## Authors
* The original work is done by [Andre Medeiros](https://github.com/staltz)
* I'm making some changes for one of my projects.