using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using GHInsights.DataFactory;
using Microsoft.Azure.Management.DataFactories.Common.Models;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Newtonsoft.Json.Linq;

namespace DataFactoryActivityExec
{
    class Program
    {
        static void Main(string[] args)
        {
            var customActivity = new MongoDbDumpTransformActivity();

            var config =
                JObject.Parse(
                    File.ReadAllText(@"..\..\..\DataFactory\Developer-KeLewis.json"));

            var linkedServices = new List<LinkedService>()
            {   new LinkedService("GHTorrentAzureStorage",
                    new LinkedServiceProperties(new CustomDataSourceLinkedService(JObject.Parse(String.Format("{{\"sasUri\": \"{0}\"}}", config["GHTorrentAzureStorage"][0]["value"].Value<string>())))))
                ,new LinkedService("GHInsightsAzureStorage",
                    new LinkedServiceProperties(new AzureStorageLinkedService(config["GHInsightsAzureStorage"][0]["value"].Value<string>())))
            };
            
            var mongoDbDump = new Dataset("MongoDbDump",
                new DatasetProperties(new AzureBlobDataset()
                {
                    FolderPath = "downloads/",
                    FileName = "mongo-dump-{Year}-{Month}-{Day}.tar.gz",
                    PartitionedBy = new List<Partition>()
                    {
                        {
                            new Partition()
                            {
                                Name = "Year",
                                Value = new DateTimePartitionValue() {Date = "SliceStart", Format = "yyyy"}
                            }
                        },
                        {
                            new Partition()
                            {
                                Name = "Month",
                                Value = new DateTimePartitionValue() {Date = "SliceStart", Format = "MM"}
                            }
                        },
                        {
                            new Partition()
                            {
                                Name = "Day",
                                Value = new DateTimePartitionValue() {Date = "SliceStart", Format = "dd"}
                            }
                        }
                    }
                },
                    new Availability("Daily", 1), "GHTorrentAzureStorage"));

            var eventDetailRawFilesBlob = new Dataset("EventDetail",
                new DatasetProperties(new AzureBlobDataset()
                {
                   FolderPath = @"test/{EventName}/v1/{Year}/{Month}",
                   FileName = "{EventName}_{Year}_{Month}_{Day}.json.gz",
                    PartitionedBy = new List<Partition>()
                    {
                        {
                            new Partition()
                            {
                                Name = "Year",
                                Value = new DateTimePartitionValue() {Date = "SliceStart", Format = "yyyy"}
                            }
                        },
                        {
                            new Partition()
                            {
                                Name = "Month",
                                Value = new DateTimePartitionValue() {Date = "SliceStart", Format = "MM"}
                            }
                        },
                        {
                            new Partition()
                            {
                                Name = "Day",
                                Value = new DateTimePartitionValue() {Date = "SliceStart", Format = "dd"}
                            }
                        }
                    }
                },
                    new Availability("Daily", 1), "GHInsightsAzureStorage"));
            

            var datasets = new List<Dataset>() { mongoDbDump, eventDetailRawFilesBlob };


            var activity = new Activity()
            {
                Description = "Fake Activity",
                Inputs = new ActivityInput[] {new ActivityInput("MongoDbDump")},
                LinkedServiceName = "BatchProcessor",
                Name = "GHTorrentEventDetailPipeline",
                Outputs = new ActivityOutput[] {new ActivityOutput("EventDetail") },
                Policy =
                    new ActivityPolicy()
                    {
                        Concurrency = 4,
                        ExecutionPriorityOrder = "NewestFirst",
                        Retry = 3,
                        Timeout = TimeSpan.Parse("04:00:00")
                    },
                Scheduler = new Scheduler("Day", 1),
                TypeProperties = new DotNetActivity("DataFactoryLib.dll"
                    , "DataFactoryLib.MongoDbDumpTransformActivity"
                    , "datafactory/DataFactoryLib.zip"
                    , "GHInsightsAzureStorage")
                {
                    ExtendedProperties = new Dictionary<string, string>()
                    {
                        {"Year", "2015"},
                        {"Month", "12"},
                        {"Day", "02"}
                    }
                }
            };

            IActivityLogger logger = new DebugLogger();



            customActivity.Execute(linkedServices, datasets, activity, logger);
        }
    }

    class DebugLogger : IActivityLogger
    {
        public void Write(string format, params object[] args)
        {
            Debug.WriteLine(String.Format(format,args));
        }
    }
}
