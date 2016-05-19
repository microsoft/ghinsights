# GHInsights
GHInsights is a dataset and processing pipeline for GitHub event and entity data. It enables you to create your own insights on all or a portion of the activity and content on GitHub.  Fundamentally GHInsights is based on [GHTorrent](http://ghtorrent.org), an open, collaborative project for gathering exposing GitHub interactions.  GHInsights takes that data and makes it available in [Azure Data Lake](https://azure.microsoft.com/en-us/solutions/data-lake/).  This gives you an easily accessible dataset and scalable compute resources so you can create the insights you need without having to gather and manage the many terabytes of data involved. 

GHInsights and the enriched datasets is exposes will evolve over time.  Currently the data available is pretty much a straight copy of that which is available in GHTorrent and the queries supplied are minimal.  We encourage the community to contribute generally useful and interesting queries and enrichments.  Those can be shared and/or incorporated directly into the GHInsights dataset and made available to everyone.

# Getting Started
Azure Data Lake is split into Storage and Analytics.  The idea with GHInsights is that we provide data in Storage and enable you to access it from your Analytics account.  This way you get full control of your analysis but on a readily available and rich dataset.  Setting up to use GHInsights has some overhead and cost.  If you want to poke around at the dataset, we recommend going to [GHTorrent](http://ghtorrent.org) and use their online dataset and query mechanism.  

*Note:  The setup here is a work in progress.  The team is actively driving to enhance and simplify.  In the future you will "mount" the database into your Data Lake account. This is both much simpler and much faster and you don't have to pay the storage costs or the query costs associated with copying the data.*

## Getting started with Hadoop
GHInsights makes the data available as Web HDFS files.  As such you can setup a Hadoop cluster and process the data.  

*Instructions coming*

## Getting started with Spark
GHInsights makes the data available as Web HDFS files.  As such you can setup a Spark cluster and process the data.  

*Instructions coming*

## Getting started with U-SQL
U-SQL is a new, SQL-like big data query language from Microsoft.

1. To get started you need to setup an [Azure Subscription] (https://azure.microsoft.com/en-us/free).

1. Request access to the dataset by contacting [@jeffmcaffer](mailto:jmcaffer@microsoft.com) and [@kelewis](mailto:kelewis@microsoft.com). We will work with you to get your Azure account enabled for Azure Data Lake Analytics (still in early preview) as well as setting up proper permission for that account to read the GitHub data.

1. Import the dataset to your account.  *Right now you have to copy the data into your account.  This is a one-time setup step that will go away as soon as Data Lake table sharing is enabled.*  To import the data, submit the `[import.usql] (https://github.com/Microsoft/ghinsights/tree/master/DataExport/import.usql)` script in your Azure Data Lake Analytics account.  This will take a while (a couple hours), once it is done you will have a copy of the GHInsights U-SQL Database in your account.

1. Run U-SQL jobs to query your data.

**Note:** In this process, the data will be copied over to your Data Lake storage. Keep in mind you are paying for the costs of storing and querying it.  Importing the core set of tables takes roughly 50 compute hours.  Pricing can vary by region and currency but is currently about US$1/hour.  By default importing skips the `CommitFile` information as it is very large and can take considerably longer (300+ compute hours).  If you want the `CommitFile` info, edit the script and uncomment the lines that fetch those files. For more Azure pricing info, see [the Azure Data Lake pricing site](https://azure.microsoft.com/en-us/pricing/details/data-lake-analytics/).


# License

GHInsights is licensed under the [MIT license](LICENSE).
