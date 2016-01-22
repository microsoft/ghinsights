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
using DataLakeStream;

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
            
            var inputLinkedService = linkedServices.Single(
                linkedService =>
                linkedService.Name ==
                inputDataset.Properties.LinkedServiceName).Properties.TypeProperties
                as AzureStorageLinkedService;

            var inConnectionString = inputLinkedService.ConnectionString; // To create an input storage client.
            var inFolderPath = GetFolderPath(inputDataset, sliceYear, sliceMonth, sliceDay);
            var inFileName = GetFileName(inputDataset, sliceYear, sliceMonth, sliceDay);
            
            
            CloudStorageAccount inputStorageAccount = CloudStorageAccount.Parse(inConnectionString);
            CloudBlobClient inputClient = inputStorageAccount.CreateCloudBlobClient();

            var inputBlobUri = new Uri(inputStorageAccount.BlobEndpoint, inFolderPath + "/" + inFileName);
            var sourceBlob = inputClient.GetBlobReferenceFromServer(inputBlobUri);

            ////////////////
            // Get output location


            var outputDataset = datasets.Single(dataset => dataset.Name == activity.Outputs.Single().Name);

            var outputLinkedService = linkedServices.Single(
                linkedService =>
                linkedService.Name ==
                outputDataset.Properties.LinkedServiceName).Properties.TypeProperties
                as AzureDataLakeStoreLinkedService;

            var outFolderPath = GetFolderPath(outputDataset, sliceYear, sliceMonth, sliceDay);
            var outFileName = GetFileName(outputDataset, sliceYear, sliceMonth, sliceDay);
            


            ////format output path string
            var outputFilenameFormatString = String.Concat(outFolderPath, "/", outFileName).Replace("{EventName}", "{0}");

            var tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
            var subscriptionId = "b44f0353-37bd-4376-bb56-351d8622535f";
            var appClientId = "42730763-ac15-41a5-a274-845e502dc23f";
            var resourceAppIdUri = "https://management.core.windows.net/";
            var managementCertificate = GetCertificate("7489979ECACD7A21E0B68B01B4180AF3159A524A");
            logger.Write("CertInfo {0} {1} {2}", managementCertificate.HasPrivateKey, managementCertificate.Thumbprint, managementCertificate.SubjectName);
            


            using (var sourceBlobStream = sourceBlob.OpenRead())
            using (var unZipStream = new System.IO.Compression.GZipStream(sourceBlobStream, System.IO.Compression.CompressionMode.Decompress))
            using (var tarStream = new TarStream.TarStream(unZipStream))
            {

                logger.Write("BlobRead: {0}", sourceBlob.Name);
                do
                {
                    var taredFileName = new System.IO.FileInfo(tarStream.CurrentFilename);

                    if (taredFileName.Extension == ".bson")
                    {
                        var tableName = taredFileName.Name.Split('.')[0];

                        var credentials = GetAppCertificateCredentials(tenantId, subscriptionId, appClientId, managementCertificate, resourceAppIdUri);
                        var dlClient = new DataLakeStoreFileSystemManagementClient(credentials);
                        var dlFrontEnd = new DataLakeStoreFrontEndAdapter("kelewis", dlClient);

                        var outputFilePath = String.Format(outputFilenameFormatString, tableName);
                        using (var dlStream = new DataLakeStream.DataLakeStream(dlFrontEnd, outputFilePath))
                        using (var gzipOut = new GZipStream(dlStream, System.IO.Compression.CompressionLevel.Optimal))
                        using (var outText = new StreamWriter(gzipOut, Encoding.UTF8, 4 * 1024 * 1024))
                        using (var reader = new BsonReader(tarStream))
                        {

                            logger.Write("BlobWrite: {0}", outputFilePath);

                            reader.CloseInput = false;

                            var jsonSerializer = new JsonSerializer();

                            var test = dlClient.GetHttpPipeline();
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

                } while (tarStream.NextFile());
            }



            // return a new Dictionary object (unused in this code).
            return new Dictionary<string, string>();
        }
        /// <summary>
        /// Gets the folderPath value from the input/output dataset.   
        /// </summary>
        private static SubscriptionCloudCredentials GetAppCertificateCredentials(string tenantId, string subscriptionId, string clientId, X509Certificate2 cert, string resourceAppIdUri)
        {
            var authority = $"https://login.windows.net/{tenantId}";
            var authContext = new Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationContext(authority);
            var clientCertCredential = new ClientAssertionCertificate(clientId, cert);


            var authResult = authContext.AcquireToken(resourceAppIdUri, clientCertCredential);
            var credentials = new TokenCloudCredentials(subscriptionId, authResult.AccessToken);
            return credentials;
        }
        private static string GetFolderPath(Dataset dataArtifact, string sliceYear, string sliceMonth, string sliceDay)
        {
            if (dataArtifact == null || dataArtifact.Properties == null)
            {
                return null;
            }

            string folderPath = null;

            if (dataArtifact.Properties.Type == "AzureBlob")
            {
                AzureBlobDataset blobDataset = dataArtifact.Properties.TypeProperties as AzureBlobDataset;

                if (blobDataset == null)
                {
                    return null;
                }
                folderPath = blobDataset.FolderPath;
            }

            if (dataArtifact.Properties.Type == "AzureDataLakeStore")
            {
                AzureDataLakeStoreDataset blobDataset = dataArtifact.Properties.TypeProperties as AzureDataLakeStoreDataset;

                if (blobDataset == null)
                {
                    return null;
                }
                folderPath = blobDataset.FolderPath;
            }

            if (folderPath != null)
                return
                    folderPath.TrimStart('/')
                        .TrimEnd('/')
                        .Replace("{Year}", sliceYear)
                        .Replace("{Month}", sliceMonth)
                        .Replace("{Day}", sliceDay);
            else
                return null;
        }
        private static X509Certificate2 GetCertificate(string thumbprint)
        {
            var store = new X509Store(StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);
            var managementCertificate = store.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, false)[0];

            return managementCertificate;
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

            string fileName = null;

            if (dataArtifact.Properties.Type == "AzureBlob")
            {
                AzureBlobDataset blobDataset = dataArtifact.Properties.TypeProperties as AzureBlobDataset;

                if (blobDataset == null)
                {
                    return null;
                }
                fileName = blobDataset.FileName;
            }

            if (dataArtifact.Properties.Type == "AzureDataLakeStore")
            {
                AzureDataLakeStoreDataset blobDataset = dataArtifact.Properties.TypeProperties as AzureDataLakeStoreDataset;

                if (blobDataset == null)
                {
                    return null;
                }
                fileName = blobDataset.FileName;
            }

            if (fileName != null)
                return
                    fileName.Replace("{Year}", sliceYear).Replace("{Month}", sliceMonth).Replace("{Day}", sliceDay);
            else
                return null;

        }
        //private static string GetContainerName(Dataset dataArtifact)
        //{
        //    if (dataArtifact == null || dataArtifact.Properties == null)
        //    {
        //        return null;
        //    }

        //    AzureBlobDataset blobDataset = dataArtifact.Properties.TypeProperties as AzureBlobDataset;
        //    if (blobDataset == null)
        //    {
        //        return null;
        //    }

        //    return blobDataset.FolderPath.Split('/')[0];
        //}

    }
}
