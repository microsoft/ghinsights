using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;

namespace GHInsights.DataFactory.Tests
{
    [TestClass]
    public class TarStreamTests
    {
        [TestMethod]
        public void TestMethod1()
        {
            using (var sourceStream = File.OpenRead(@"C:\Data\mongo-dump-2016-01-14.tar.gz"))
            using (var unZipStream = new System.IO.Compression.GZipStream(sourceStream, System.IO.Compression.CompressionMode.Decompress))
            using (var tarStream = new TarStream(unZipStream))
            {

                while (tarStream.NextFile())
                {
                    var taredFileExtention = Path.GetExtension(tarStream.CurrentFilename);

                    if (tarStream.CurrentFilename == "dump/github/org_members.bson") //taredFileExtention == ".bson")
                    {
                        using (var output = File.Create("org_members.bson"))
                        {
                            Byte[] outputBytes = new byte[8196];
                            while (tarStream.Read(outputBytes, 0, 8196) > 0)
                            {
                                
                                output.Write(outputBytes, 0, outputBytes.Length);
                            }
                        }
                    }
                };
            }
        }
    }
}
