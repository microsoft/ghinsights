Data Export & Import
====================

Summary
-------
These instructions show the steps to enable other ADLA accounts to be able to use the GitHub data.  To begin you'll need an [Azure Subscription] (https://azure.microsoft.com/en-us/free).

1) Contact @jeffmcaffer and @kelewis requesting access. We will work with you to get your Azure account enabled for Azure Data Lake Analytics (still in early preview) as well as setting up proper permission for that account to read our GitHub data.

2) Submit the **[import.usql] (https://github.com/Microsoft/ghinsights/tree/master/DataExport/import.usql)** script in your Azure Data Lake Analytics account.  This will take a while, once it is done you will have a copy of the **GHInsights** U-SQL Database in your account.

3) Run U-SQL jobs to query your data.

**Note:** The data is copied over to your ADLA Catalog (which uses ADLS for storage). Keep in mind you are paying for the costs of storing and querying it.  Importing the core set of tables takes roughly 50 compute hours.  Pricing can very by region and currency but currently this is about 50$USD of compute time.  Importing CommitFile information can take considerably longer (300+ compute hours).  See [the Azure Data Lake pricing site] (https://azure.microsoft.com/en-us/pricing/details/data-lake-analytics/) for the most up to date information.

In the Future
-------------
The process of initial setup and sharing of the **GHInsights** database will be much simpler. You will “mount” the database into your ADLA account. This is both much simpler and much faster. Also you don’t have to pay the storage costs or the query costs associated with copying the data.
