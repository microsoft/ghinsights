using System;
using Microsoft.Azure.Management.DataFactories.Models;
using Newtonsoft.Json.Linq;

namespace GHInsights.DataFactory
{
    internal class BlobLocation
    {
        private Dataset _dataset;
        private LinkedService _linkedService;
        private string _sliceDay;
        private string _sliceMonth;
        private string _sliceYear;


        public BlobLocation(LinkedService linkedService, Dataset dataset, string sliceYear, string sliceMonth,
            string sliceDay)
        {
            _linkedService = linkedService;
            _dataset = dataset;
            _sliceYear = sliceYear;
            _sliceMonth = sliceMonth;
            _sliceDay = sliceDay;


            
        }

        public string BlobFullPath
        {
            get { return (String.IsNullOrWhiteSpace(this.FolderPath) ? null : this.FolderPath + "/") + this.FileName; }
        }

        public Uri ConnectionSasUri
        {
            get
            {
                return
                    new Uri(
                        (_linkedService.Properties.TypeProperties as CustomDataSourceLinkedService)
                            .ServiceExtraProperties["sasUri"].Value<string>()); // To create an input storage client.
            }
        }

        public string ConnectionString
        {
            get { return (_linkedService.Properties.TypeProperties as AzureStorageLinkedService).ConnectionString; }
        }

        /// <summary>
        /// Gets the folderPath value from the input/output dataset.   
        /// </summary>

        public string FolderPath
        {
            get
            {
                if (_dataset == null || _dataset.Properties == null)
                {
                    return null;
                }

                AzureBlobDataset blobDataset = _dataset.Properties.TypeProperties as AzureBlobDataset;
                if (blobDataset == null)
                {
                    return null;
                }

                return
                    blobDataset.FolderPath.Replace(this.ContainerName, "")
                        .TrimStart('/')
                        .TrimEnd('/')
                        .Replace("{Year}", _sliceYear)
                        .Replace("{Month}", _sliceMonth)
                        .Replace("{Day}", _sliceDay);
            }
        }



        public string FileName
        {
            get
            {
                if (_dataset == null || _dataset.Properties == null)
                {
                    return null;
                }

                AzureBlobDataset blobDataset = _dataset.Properties.TypeProperties as AzureBlobDataset;
                if (blobDataset == null)
                {
                    return null;
                }

                return blobDataset.FileName.Replace("{Year}", _sliceYear)
                    .Replace("{Month}", _sliceMonth)
                    .Replace("{Day}", _sliceDay);
            }
        }

        public string ContainerName
        {
            get
            {
                if (_dataset == null || _dataset.Properties == null)
                {
                    return null;
                }

                AzureBlobDataset blobDataset = _dataset.Properties.TypeProperties as AzureBlobDataset;
                if (blobDataset == null)
                {
                    return null;
                }

                return blobDataset.FolderPath.Split('/')[0];
            }
        }
    }
}