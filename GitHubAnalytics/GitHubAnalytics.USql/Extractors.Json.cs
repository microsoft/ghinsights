using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
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
                    
                    var data = new Dictionary<string, string>();

                    foreach (var column in FlattenJson(row))
                    {
                        if (column.Value != null)
                        {
                            var outString = column.Value.ToString();

                            if (Encoding.UTF8.GetBytes(outString).Length <= 100000)
                            {
                                data[column.Key] = outString;
                            }
                            else
                            {
                                data[column.Key] = String.Format("##StringTooLong##,{0}", Encoding.UTF8.GetBytes(outString).Length);
                            }
                        }
                    }

                    output.Set(_outputColumnName, new SqlMap<string, string>(data));

                    yield return output.AsReadOnly();

                }
            }
        }

        private IDictionary<string, object> FlattenJson(JToken input)
        {
        
            Dictionary<string, object> dict = new Dictionary<string, object>();

            FlattenJson(input, dict);

            return dict;

        }


        private void FlattenJson(JToken input, Dictionary<string, object> dict)
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
                dict.Add(input.Path, input.Value<string>());
            }

        }
    }
}
