using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories;
using Microsoft.Azure.Management.DataFactories.Common.Models;
using Microsoft.Azure.Management.DataFactories.Core.Registration.Models;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DataFactoryActivityExec
{
    class Program
    {
        static void Main(string[] args)
        {
            var customActivity = new MongoDbDumpTransformActivity.MongoDbDumpTransformActivity();


            var linkedServices = new List<LinkedService>()
            {   new LinkedService("GHTorrentAzureStorage",
                    new LinkedServiceProperties(new CustomDataSourceLinkedService(JObject.Parse("{\"sasUri\": \"https://ghtstorage.blob.core.windows.net/downloads?restype=container&comp=list&sv=2015-04-05&sr=c&sig=VaU0Tw0n9uUO77hoeIty0yzHI8G%2FQ2eyoo99CQ8Q0%2FI%3D&se=2018-02-02T22%3A02%3A16Z&sp=rl\"}"))))
                ,new LinkedService("GitHubAnalyticsAzureStorage",
                    new LinkedServiceProperties(new AzureStorageLinkedService("DefaultEndpointsProtocol=https;AccountName=kelewis;AccountKey=hCvPmKWV532FXwo+S1XvTeq64QDt/ibWdewU2oHDSrs9slXaHIHpbxUCHL99WF/1O4AR53WhHHOKu2BKf3UNbA==")))
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

            var eventDetailRawFilesBlob = new Dataset("EventDetailRawFilesBlob",
                new DatasetProperties(new AzureBlobDataset()
                {
                    FolderPath = @"raw/MongoDump/V1/{EventName}/{Year}/{Month}",
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
                    new Availability("Daily", 1), "GitHubAnalyticsAzureStorage"));
            

            var datasets = new List<Dataset>() { mongoDbDump, eventDetailRawFilesBlob };


            var activity = new Activity()
            {
                Description = "<Enter the Pipeline description here>",
                Inputs = new ActivityInput[] {new ActivityInput("MongoDbDump")},
                LinkedServiceName = "BatchProcessor",
                Name = "GHTorrentEventDetailPipeline",
                Outputs = new ActivityOutput[] {new ActivityOutput("EventDetailRawFilesBlob") },
                Policy =
                    new ActivityPolicy()
                    {
                        Concurrency = 4,
                        ExecutionPriorityOrder = "NewestFirst",
                        Retry = 3,
                        Timeout = TimeSpan.Parse("04:00:00")
                    },
                Scheduler = new Scheduler("Day", 1),
                //Type = "MongoDbDumpTransformActivity",
                TypeProperties = new DotNetActivity("MongoDbDumpTransformActivity.dll"
                    , "MongoDbDumpTransformActivity.MongoDbDumpTransformActivity"
                    , "datafactory/MongoDbDumpTransformActivity.zip"
                    , "GitHubAnalyticsAzureStorage")
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
