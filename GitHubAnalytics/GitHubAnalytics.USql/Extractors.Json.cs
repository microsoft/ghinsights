using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GitHubAnalytics.USql
{
    public class Extractors
    {
        public static IExtractor FlatJson(string outputColumnName)
        {
            return new FlatJsonExtractor(outputColumnName);
        }
    }
    
    [SqlUserDefinedExtractor(AtomicFileProcessing = true)]
    internal class FlatJsonExtractor : IExtractor
    {
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
                    
                    output.Set(_outputColumnName, new SqlMap<string, SqlArray<byte[]>>(FlattenJson(row)));

                    yield return output.AsReadOnly();

                }
            }
        }

        private IDictionary<string, SqlArray<byte[]>> FlattenJson(JToken input)
        {
        
            var dict = new Dictionary<string, SqlArray<byte[]>>();

            FlattenJson(input, dict);

            return dict;

        }


        private void FlattenJson(JToken input, Dictionary<string, SqlArray<byte[]>> dict)
        {
            if (input.Type == JTokenType.Object || input.Type == JTokenType.Property || input.Type == JTokenType.Array)
            {
                foreach (var v in input.Children())
                {

                    FlattenJson(v, dict);

                }
            }
            else
            {
                if (!String.IsNullOrWhiteSpace(input.Value<string>()))
                {
                    SqlArray<byte[]> data =
                        new SqlArray<byte[]>(new byte[][] { Encoding.UTF8.GetBytes(input.Value<string>()), Encoding.UTF8.GetBytes(input.Type.ToString())});
                    dict.Add(input.Path, data);
                }
            }

        }
    }
}
