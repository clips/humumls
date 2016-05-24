# humumls
UMLS in Python with MongoDB.

This package allows for the conversion of UMLS .RRF files to a document-level MongoDB database. Note that the goal of this package is not to provide a 1-to-1 mapping from the UMLS .RRF mapping to a MongoDB, but rather to provide useful abstractions where necessary.

## Important changes

* The Atom Unique Identifier (AUI) has been removed, as this is no longer necessary when using document-level databases.
* There is no distinction between different sources, as the SAB field has been removed.
* All strings with a STR field over 1000 bytes in length are considered noisy, and removed.
* Opaque field names, such as relations, have been expanded to their natural language alternatives.
* Definitions are tagged with a language using langid.
* All information not in MRDEF, MRCONSO or MRREL is discarded.

## Usage

First, make sure that you have a `MongoDB` instance running, and that you know
on which port and hostname it is running.

The `tablecreator.py` file contains the code for creating the `MongoDB`.

```python
from tablecreator import TableCreator

# Get all English and Dutch items.
languages = ["ENG", "DUT"]

# Use default port, host and name.
t = TableCreator(languages, "path/to/meta")

# Create the tables. This takes ~2 hours on a normal laptop.
t.process(verbose=True)
```

This `MongoDB` can be addressed using `pymongo`, or the provided `aggregate` class. The aggregate class is meant to be expanded for your specific purposes. Currently, it contains examples of use.

```python

from aggregator import Aggregator

# You can define your own aggregate queries.
agg = Aggregator()

# Query using connections.
concept = agg.concept["C0032344"]
string = agg.string["S000124"]

# Perform aggregate queries.
cancers = agg.concept_string["cancer"]
```
