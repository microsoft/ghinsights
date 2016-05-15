using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.IO.Compression;

namespace GitHubAnalytics.USql
{
    public class Extractors
    {
        public static IExtractor FlatJson(string outputColumnName)
        {
            return new FlatJsonExtractor(outputColumnName);
        }
    }
    
    [SqlUserDefinedExtractor(AtomicFileProcessing = false)]
    internal class FlatJsonExtractor : IExtractor
    {
        private const int _dataLakeMaxRowSize = 4194304;

        private string _outputColumnName;

        internal FlatJsonExtractor(string outputColumnName)
        {
            _outputColumnName = outputColumnName;
        }
        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            using (var reader = new JsonTextReader(new StreamReader(input.BaseStream, Encoding.UTF8)))
            {
                reader.SupportMultipleContent = true;
                
                while (reader.Read())
                {
                    var row = JToken.ReadFrom(reader);

                    var size = 0;
                    var flattendData = GitHubAnalytics.USql.Utility.FlattenJson(row, ref size);

                    if (size < (_dataLakeMaxRowSize))
                    {
                        output.Set(_outputColumnName, new SqlMap<string, byte[]>(flattendData));
                    }
                    else
                    {
                        var compressedData = GitHubAnalytics.USql.Utility.GzipByteArray(Encoding.UTF8.GetBytes(row.ToString(Formatting.None)));

                        if (compressedData.Length < (_dataLakeMaxRowSize))
                        {
                            var compressedRow = new Dictionary<string, byte[]>
                                {
                                    {
                                        "!CompressedRow",
                                        compressedData
                                    }
                                };
                            output.Set(_outputColumnName, new SqlMap<string, byte[]>(compressedRow));
                        }
                        else {
                            //throw new ArgumentOutOfRangeException($"Resulting SqlMap is too large: {size} - {row.ToString(Formatting.None).Substring(0,100)}");
                            var error = new Dictionary<string, byte[]>
                                {
                                    {
                                        "!RowExtractorError",
                                        Encoding.UTF8.GetBytes($"Resulting SqlMap is too large: OriginalSize:{size} CompressedSize: {compressedData.Length} - {row.ToString(Formatting.None).Substring(0, 100)}")
                                    }
                                };
                            output.Set(_outputColumnName, new SqlMap<string, byte[]>(error));
                        }

                    }
                        

                    yield return output.AsReadOnly();

                }
            }
        }

    }


}
