using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Analytics.Types.Sql;
using Newtonsoft.Json.Linq;
using System.IO;
using System.IO.Compression;

namespace GHInsights.USql
{
    public class Utility
    {
        public const int MaxUSqlStringByteLength = 1024 * 127;

        public static string Left(string value, int length)
        {
            if(value == null)
            {
                return null;
            }

            if (value.Length > length)
            {
                return value.Substring(0, length);
            }
            return value;
        }

        public static string Concat(params string[] inputStrings)
        {
            for(int i=0;i<inputStrings.Length;i++)
            {
                if(inputStrings[i] == null)
                {
                    inputStrings[i] = "";
                }
            }

            var valueString = String.Concat(inputStrings);
            
            return valueString;
        }

        public static string GetUSqlString(byte[] inputColumn)
        {
            if(inputColumn == null)
            {
                return null;
            }

            var valueString = Encoding.UTF8.GetString(inputColumn, 0, Math.Min(MaxUSqlStringByteLength, inputColumn.Length));

            return valueString;
        }

        public static string GetUSqlString(SqlMap<string, byte[]> inputColumn, string path)
        {
            return GetUSqlString(GetRawBytes(inputColumn, path));
        }

        public static string GetString(SqlMap<string, byte[]> inputColumn, string path)
        {
            return GetString(inputColumn, path, null);
        }

        public static string GetString(SqlMap<string, byte[]> inputColumn, string path, int? count)
        {
            var valueString = GetValue(inputColumn, path, count);

            if (valueString == null)
            {
                return null;
            }

            if (valueString.Length > MaxUSqlStringByteLength)
            {
                throw new ArgumentOutOfRangeException($"String too long: {valueString.Length} - {path}");
            }
            return valueString;

        }

        public static string GetPiiString(SqlMap<string, byte[]> inputColumn, string path)
        {
            var inputString = GetString(inputColumn, path);

            return HashEmail(inputString);
        }


        public static bool? GetBoolean(SqlMap<string, byte[]> inputColumn, string path)
        {
            var value = GetValue(inputColumn, path);
            if (value == null)
            {
                return null;
            }

            try
            {
                Boolean booleanValue;
                Boolean.TryParse(value, out booleanValue);
                return booleanValue;
            }
            catch (FormatException)
            {
                throw new FormatException($"Error trying to parse using GetBoolean - {value}");
            }
        }

        public static DateTime? GetDateTime(SqlMap<string, byte[]> inputColumn, string path)
        {
            var value = GetValue(inputColumn, path);
            if (value == null)
            {
                return null;
            }

            try
            {
                DateTime dateTime;
                DateTime.TryParse(value, out dateTime);
                return dateTime;
            }
            catch (FormatException)
            {
                throw new FormatException($"Error trying to parse using GetDateTime - {value}");
            }
        }
        public static Guid? GetGuid(SqlMap<string, byte[]> inputColumn, string path)
        {
            var value = GetValue(inputColumn, path);
            if (value == null)
            {
                return null;
            }

            try
            {
                Guid guid;
                Guid.TryParse(value, out guid);
                return guid;
            }
            catch (FormatException)
            {
                throw new FormatException($"Error trying to parse using GetDateTime - {value}");
            }
        }

        public static Int64? GetInteger(SqlMap<string, byte[]> inputColumn, string path)
        {
            var value = GetValue(inputColumn, path);
            if (value == null)
            {
                return null;
            }
            return Int64.Parse(value);
        }

        public static byte[] GetBytes(SqlMap<string, byte[]> inputColumn, string path)
        {
            var value = GetValue(inputColumn, path);
            if (value == null)
            {
                return null;
            }
            return Convert.FromBase64String(value);
        }

        public static byte[] GetRawBytes(SqlMap<string, byte[]> inputColumn, string path)
        {
            if (inputColumn.ContainsKey(path))
            {
                var value = inputColumn[path];
                return value;
            }
            return null;
        }

        private static string GetValue(SqlMap<string, byte[]> inputColumn, string path, int? count = null)
        {
            if (inputColumn.ContainsKey(path))
            {
                if (count == null)
                {
                    return Encoding.UTF8.GetString(inputColumn[path]);
                }else
                {
                    return Encoding.UTF8.GetString(inputColumn[path], 0, Math.Min(MaxUSqlStringByteLength, inputColumn[path].Length));
                }
            }
            return null;
        }

        public static string HashEmail(string inString)
        {
            if (inString == null)
            {
                return null;
            }

            var regex =
                new Regex(
                    @"(?<alias>\A[a-z0-9!# \.$%&'*+/=?^_`{|}~-]+(?<alias>:\.[a-z0-9!# \.$%&'*+/=?^_`{|}~-]+)*)@(?<domain>(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9]))?\z", RegexOptions.CultureInvariant);
            var matches = regex.Matches(inString.Trim().ToLowerInvariant());
            if (matches.Count >= 1)
            {
                var sha = new SHA256Managed();
                var shaString = BitConverter.ToString(sha.ComputeHash(Encoding.UTF8.GetBytes(matches[0].Groups["alias"].Value))).Replace("-","").ToLowerInvariant();
                return String.Concat(shaString, "@", matches[0].Groups["domain"].Value);
            }

            return inString;
        }
        
        public static IDictionary<string, byte[]> FlattenJson(JToken input, ref int size)
        {

            var dict = new Dictionary<string, byte[]>();

            FlattenJson(input, dict, ref size);

            return dict;

        }

        private static void FlattenJson(JToken input, Dictionary<string, byte[]> dict, ref int size)
        {
            if (input.Type == JTokenType.Object || input.Type == JTokenType.Property || input.Type == JTokenType.Array)
            {
                foreach (var v in input.Children())
                {

                    FlattenJson(v, dict, ref size);

                }
            }
            else
            {
                if (!String.IsNullOrWhiteSpace(input.Value<string>()))
                {
                    var data = Encoding.UTF8.GetBytes(input.Value<string>());

                    size += Encoding.UTF8.GetBytes(input.Path).Length + data.Length;
                    size += 100;
                    dict.Add(input.Path, data);
                }
            }
        }

        public static byte[] GzipByteArray(byte[] bytes)
        {
            using (var msi = new MemoryStream(bytes))
            using (var mso = new MemoryStream())
            {
                using (var gs = new GZipStream(mso, CompressionMode.Compress))
                {
                    msi.CopyTo(gs);
                }

                return mso.ToArray();
            }
        }

        public static byte[] GunzipByteArray(byte[] bytes)
        {
            using (var msi = new MemoryStream(bytes))
            using (var mso = new MemoryStream())
            {
                using (var gs = new GZipStream(msi, CompressionMode.Decompress))
                {
                    gs.CopyTo(mso);
                }

                return mso.ToArray();
            }
        }

        public static byte[] TruncateByteArray(byte[] array, int maxSize)
        {
            if (array.Length > maxSize)
            {
                Array.Resize(ref array, maxSize);
            }

            return array;
        }

    }
}
