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

namespace GHInsights.USql
{
    public class Extractors
    {
        public static IExtractor FlatJson(string outputColumnName, bool silent = false)
        {
            return new FlatJsonExtractor(outputColumnName, silent);
        }
    }
    
    [SqlUserDefinedExtractor(AtomicFileProcessing = false)]
    internal class FlatJsonExtractor : IExtractor
    {
        private const int _dataLakeMaxRowSize = 4194304;

        private string _outputColumnName;
        private readonly bool _silent;

        internal FlatJsonExtractor(string outputColumnName, bool silent)
        {
            _outputColumnName = outputColumnName;
            _silent = silent;
        }

        private JToken TryReadFrom(StreamReader input, bool silent)
        {
            var inString = input.ReadLine();
            if (!String.IsNullOrWhiteSpace(inString))
            {
                try
                {
                    return JToken.Parse(inString);
                }
                catch (JsonReaderException)
                {
                    if (silent)
                    {
                        return new JProperty("_parse_error", inString);
                    }
                    else
                    {
                        throw new FormatException(String.Format("Unable to parse JSON token at position {0} - {1}",  + input.BaseStream.Position, inString));
                    }
                }
            }
            return null;
        }


        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            using (StreamReader inputStream = new StreamReader(input.BaseStream))
            {
                while (!inputStream.EndOfStream)
                {
                    var row = TryReadFrom(inputStream, _silent);
                    if (row != null)
                    {
                        var size = 0;
                        var flattendData = GHInsights.USql.Utility.FlattenJson(row, ref size);

                        if (size < (_dataLakeMaxRowSize))
                        {
                            output.Set(_outputColumnName, new SqlMap<string, byte[]>(flattendData));
                        } else
                        {
                            var compressedData = GHInsights.USql.Utility.GzipByteArray(Encoding.UTF8.GetBytes(row.ToString(Formatting.None)));

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
                            } else
                            {
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
}
