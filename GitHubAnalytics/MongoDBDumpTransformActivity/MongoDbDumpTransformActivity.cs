using Microsoft.Azure.Management.DataFactories.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Runtime.Remoting.Metadata.W3cXsd2001;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.Azure.Management.DataFactories;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataLake.StoreFileSystem;
using Microsoft.Azure.Management.DataLake.StoreUploader;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System.Web;
using Hyak.Common;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;

namespace MongoDbDumpTransformActivity
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
            //logger.Write("datasets: {0}", JsonConvert.SerializeObject(datasets));
            //logger.Write("activity: {0}", JsonConvert.SerializeObject(activity));

            var sliceYear = ((DotNetActivity) activity.TypeProperties).ExtendedProperties["Year"];
            var sliceMonth = ((DotNetActivity)activity.TypeProperties).ExtendedProperties["Month"];
            var sliceDay = ((DotNetActivity)activity.TypeProperties).ExtendedProperties["Day"];

            /////////////////
            // Open up input Blob

            var inputDataset = datasets.Single(dataset => dataset.Name == activity.Inputs.Single().Name);
            //TODO: first vs single
            var inputLinkedService = linkedServices.Single(
                linkedService =>
                linkedService.Name ==
                inputDataset.Properties.LinkedServiceName).Properties.TypeProperties
                as CustomDataSourceLinkedService;

            var inConnectionString = inputLinkedService.ServiceExtraProperties["sasUri"].Value<string>(); // To create an input storage client.
            var inContainerName = GetContainerName(inputDataset);
            var inFolderPath = GetFolderPath(inputDataset, sliceYear, sliceMonth, sliceDay);
            var inFileName = GetFileName(inputDataset, sliceYear, sliceMonth, sliceDay);

            // TODO: clean up prep and parsing functionality into new specific types

            //CloudStorageAccount inputStorageAccount = CloudStorageAccount.Parse(inConnectionString);
            //CloudBlobClient inputClient = inputStorageAccount.CreateCloudBlobClient();
            //var inputBlobUri = new Uri(inputStorageAccount.BlobEndpoint, inContainerName + (String.IsNullOrWhiteSpace(inFolderPath)?null:"/" + inFolderPath) + "/" + inFileName);
            //var sourceBlob = inputClient.GetBlobReferenceFromServer(inputBlobUri);

            var inputContainer = new CloudBlobContainer(new Uri(inConnectionString));
            var inputBlobFullPath = (String.IsNullOrWhiteSpace(inFolderPath) ? null : inFolderPath + "/")  + inFileName;

            var sourceBlob = inputContainer.GetBlobReferenceFromServer(inputBlobFullPath);

            ////////////////
            // Get output location


            var outputDataset = datasets.Single(dataset => dataset.Name == activity.Outputs.Single().Name);

            var outputLinkedService = linkedServices.Single(
                linkedService =>
                linkedService.Name ==
                outputDataset.Properties.LinkedServiceName).Properties.TypeProperties
                as AzureStorageLinkedService;

            var outConnectionString = outputLinkedService.ConnectionString; // To create an input storage client.
            var outFolderPath = GetFolderPath(outputDataset, sliceYear, sliceMonth, sliceDay);
            var outFileName = GetFileName(outputDataset, sliceYear, sliceMonth, sliceDay);
            var outContainerName = GetContainerName(outputDataset);


            CloudStorageAccount outputStorageAccount = CloudStorageAccount.Parse(outConnectionString);
            CloudBlobClient outputClient = outputStorageAccount.CreateCloudBlobClient();
            var outContainer = outputClient.GetContainerReference("raw");
            outContainer.CreateIfNotExists();

            //format output path string
            var outputFilenameFormatString = String.Concat(outFolderPath, "/", outFileName).Replace("{EventName}", "{0}");





            using (var sourceBlobStream = sourceBlob.OpenRead())
            using (var unZipStream = new System.IO.Compression.GZipStream(sourceBlobStream, System.IO.Compression.CompressionMode.Decompress))
            using (var tarStream = new TarStream.TarStream(unZipStream))
            {

                logger.Write("BlobRead: {0}", sourceBlob.Name);
                do // TODO: while at top.. call nextfile() here, setting up the first file (if one exists)
                {
                    var taredFileName = new System.IO.FileInfo(tarStream.CurrentFilename);
                    // TODO: use to get names and extentions Path.GetFileNameWithoutExtension()

                    if (taredFileName.Extension == ".bson")
                    {
                        //TODO: redo all this with Path
                        var tableName = taredFileName.Name.Split('.')[0];
                        
                        var outputBlob = outContainer.GetBlockBlobReference(String.Format(outputFilenameFormatString,tableName));

                        using (var outBlobStream = outputBlob.OpenWrite())
                        using (var gzipOut = new GZipStream(outBlobStream, System.IO.Compression.CompressionLevel.Optimal))
                        using (var outText = new StreamWriter(gzipOut, Encoding.UTF8))
                        using (var reader = new BsonReader(tarStream))
                        {

                            logger.Write("BlobWrite: {0}/{1}", outContainerName, outputBlob.Name);

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
                    //TODO: flip the while to the top
                } while (tarStream.NextFile());
            }



            // return a new Dictionary object (unused in this code).
            return new Dictionary<string, string>();
        }
        /// <summary>
        /// Gets the folderPath value from the input/output dataset.   
        /// </summary>

        private static string GetFolderPath(Dataset dataArtifact, string sliceYear, string sliceMonth, string sliceDay)
        {
            if (dataArtifact == null || dataArtifact.Properties == null)
            {
                return null;
            }

            AzureBlobDataset blobDataset = dataArtifact.Properties.TypeProperties as AzureBlobDataset;
            if (blobDataset == null)
            {
                return null;
            }

            return blobDataset.FolderPath.Replace(GetContainerName(dataArtifact),"").TrimStart('/').TrimEnd('/').Replace("{Year}", sliceYear).Replace("{Month}", sliceMonth).Replace("{Day}", sliceDay);
        }


        /// <summary>
        /// Gets the fileName value from the input/output dataset.   
        /// </summary>

        private static string GetFileName(Dataset dataArtifact, string sliceYear, string sliceMonth, string sliceDay)
        {
            if (dataArtifact == null || dataArtifact.Properties == null)
            {
                return null;
            }

            AzureBlobDataset blobDataset = dataArtifact.Properties.TypeProperties as AzureBlobDataset;
            if (blobDataset == null)
            {
                return null;
            }

            return blobDataset.FileName.Replace("{Year}", sliceYear).Replace("{Month}", sliceMonth).Replace("{Day}", sliceDay);
        }
        private static string GetContainerName(Dataset dataArtifact)
        {
            if (dataArtifact == null || dataArtifact.Properties == null)
            {
                return null;
            }

            AzureBlobDataset blobDataset = dataArtifact.Properties.TypeProperties as AzureBlobDataset;
            if (blobDataset == null)
            {
                return null;
            }

            return blobDataset.FolderPath.Split('/')[0];
        }

    }
}
