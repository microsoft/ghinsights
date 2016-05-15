using Microsoft.Azure.Management.DataFactories.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;

namespace GHInsights.DataFactory
{
    public class MongoDbDumpTransformActivity : IDotNetActivity
    {
        /// <summary>
        /// Execute method is the only method of IDotNetActivity interface you must implement. 
        /// In this sample, the method invokes the Calculate method to perform the core logic.  
        /// </summary>

        public IDictionary<string, string> Execute(
            IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets,
            Activity activity,
            IActivityLogger logger)
        {
            
            /////////////////
            // Log input parameters

            // to get extended properties (for example: SliceStart)
            foreach (LinkedService ls in linkedServices)
            {
                logger.Write("linkedServices: {0}, {1}, {2}, {3}", ls.Name, ls.Properties.Type,
                    ls.Properties.Description, ls.Properties.ErrorMessage);
            }



            var sliceYear = ((DotNetActivity)activity.TypeProperties).ExtendedProperties["Year"];
            var sliceMonth = ((DotNetActivity)activity.TypeProperties).ExtendedProperties["Month"];
            var sliceDay = ((DotNetActivity)activity.TypeProperties).ExtendedProperties["Day"];

            logger.Write("dataSlice: {0}-{1}-{2}", sliceYear, sliceMonth, sliceDay);


            /////////////////
            // Open up input Blob


            var inputDataset = datasets.Single(dataset => dataset.Name == activity.Inputs.Single().Name);
            var inputLinkedService = linkedServices.Single(
                linkedService =>
                linkedService.Name ==
                inputDataset.Properties.LinkedServiceName);

            var inputLocation = new BlobLocation(inputLinkedService, inputDataset, sliceYear, sliceMonth, sliceDay);


            var inputContainer = new CloudBlobContainer(inputLocation.ConnectionSasUri);
            var sourceBlob = inputContainer.GetBlobReferenceFromServer(inputLocation.BlobFullPath);

            ////////////////
            // Get output location

            var outputDataset = datasets.Single(dataset => dataset.Name == activity.Outputs.Single().Name);

            var outputLinkedService = linkedServices.Single(
                linkedService =>
                linkedService.Name ==
                outputDataset.Properties.LinkedServiceName);

            var outputLocation = new BlobLocation(outputLinkedService, outputDataset, sliceYear, sliceMonth, sliceDay);
            
            CloudStorageAccount outputStorageAccount = CloudStorageAccount.Parse(outputLocation.ConnectionString);
            CloudBlobClient outputClient = outputStorageAccount.CreateCloudBlobClient();
            var outContainer = outputClient.GetContainerReference(outputLocation.ContainerName);

            outContainer.CreateIfNotExists();

            //format output path string
            var outputFilenameFormatString = outputLocation.BlobFullPath;


            using (var sourceBlobStream = sourceBlob.OpenRead())
            using (var unZipStream = new System.IO.Compression.GZipStream(sourceBlobStream, System.IO.Compression.CompressionMode.Decompress))
            using (var tarStream = new TarStream(unZipStream))
            {

                logger.Write("BlobRead: {0}/{1}", inputLocation.ContainerName, inputLocation.BlobFullPath);
                while (tarStream.NextFile())
                {
                    var tableName = Path.GetFileNameWithoutExtension(tarStream.CurrentFilename);
                    var taredFileExtention = Path.GetExtension(tarStream.CurrentFilename);
                    
                    if (taredFileExtention == ".bson")
                    {
                        
                        var outputBlob = outContainer.GetBlockBlobReference(outputFilenameFormatString.Replace("{EventName}", tableName));

                        using (var outBlobStream = outputBlob.OpenWrite())
                        using (var gzipOut = new GZipStream(outBlobStream, System.IO.Compression.CompressionLevel.Optimal))
                        using (var outText = new StreamWriter(gzipOut, Encoding.UTF8))
                        using (var reader = new BsonReader(tarStream))
                        {

                            logger.Write("BlobWrite: {0}/{1}", outputLocation.ContainerName, outputBlob.Name);

                            reader.CloseInput = false;

                            var jsonSerializer = new JsonSerializer();


                            reader.ReadRootValueAsArray = false;
                            reader.SupportMultipleContent = true;

                            while (reader.Read())
                            {
                                var row = (JObject)jsonSerializer.Deserialize(reader);

                                var outString = row.ToString(Formatting.None);

                                outText.WriteLine(outString);
                            }
                        }
                    }
                } ;
            }



            // return a new Dictionary object
            return new Dictionary<string, string>();
        }
    }
}
