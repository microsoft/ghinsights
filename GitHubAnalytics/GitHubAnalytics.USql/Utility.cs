using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Analytics.Types.Sql;

namespace GitHubAnalytics.USql
{
    public class Utility
    {
        public static string GetString(SqlMap<string, byte[]> inputColumn, string path)
        {
            var valueString = GetValue(inputColumn, path);
            if (valueString?.Length > 127000)
            {
                throw new ArgumentOutOfRangeException($"String too long: {valueString.Length} - {path}");
            }
            return valueString;

        }
        public static string GetString(SqlMap<string, SqlArray<byte[]>> inputColumn, string path)
        {
            var valueString = GetValue(inputColumn, path);
            if (valueString?.Length > 127000)
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

        public static string GetPiiString(SqlMap<string, SqlArray<byte[]>> inputColumn, string path)
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
            return Boolean.Parse(value);
        }

        public static DateTime? GetDateTime(SqlMap<string, byte[]> inputColumn, string path)
        {
            var value = GetValue(inputColumn, path);
            if (value == null)
            {
                return null;
            }
            return DateTime.Parse(value);
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

        public static bool? GetBoolean(SqlMap<string, SqlArray<byte[]>> inputColumn, string path)
        {
            var value = GetValue(inputColumn, path);
            if (value == null)
            {
                return null;
            }
            return Boolean.Parse(value);
        }

        public static DateTime? GetDateTime(SqlMap<string, SqlArray<byte[]>> inputColumn, string path)
        {
            var value = GetValue(inputColumn, path);
            if (value == null)
            {
                return null;
            }
            return DateTime.Parse(value);
        }

        public static Int64? GetInteger(SqlMap<string, SqlArray<byte[]>> inputColumn, string path)
        {
            var value = GetValue(inputColumn, path);
            if (value == null)
            {
                return null;
            }
            return Int64.Parse(value);
        }

        public static byte[] GetBytes(SqlMap<string, SqlArray<byte[]>> inputColumn, string path)
        {
            var value = GetValue(inputColumn, path);
            if (value == null)
            {
                return null;
            }
            return Convert.FromBase64String(value);
        }
        public static byte[] GetRawBytes(SqlMap<string, SqlArray<byte[]>> inputColumn, string path)
        {
            if (inputColumn.ContainsKey(path) && inputColumn[path].Count > 0)
            {
                var value = inputColumn[path][0];
                return value;
            }
            return null;
        }

        private static string GetValue(SqlMap<string, SqlArray<byte[]>> inputColumn, string path)
        {
            if (inputColumn.ContainsKey(path) && inputColumn[path].Count > 0)
            {
                var value = Encoding.UTF8.GetString(inputColumn[path][0]);
                return value;
            }
            return null;
        }
        private static string GetValue(SqlMap<string, byte[]> inputColumn, string path)
        {
            if (inputColumn.ContainsKey(path))
            {
                var value = Encoding.UTF8.GetString(inputColumn[path]);
                return value;
            }
            return null;
        }

        public static string GetType(SqlMap<string, SqlArray<byte[]>> inputColumn, string path)
        {
            if (inputColumn.ContainsKey(path) && inputColumn[path].Count > 1)
            {
                var type = Encoding.UTF8.GetString(inputColumn[path][1]);
                return type;
            }
            return null;
        }

        private static string HashEmail(string inString)
        {
            var regex =
                new Regex(
                    @"(?<alias>\A[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?<alias>:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*)@(?<domain>(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9]))?\z", RegexOptions.CultureInvariant);
            var matches = regex.Matches(inString);
            if (matches.Count > 1)
            {
                var sha = new SHA256Managed();
                var shaString = BitConverter.ToString(sha.ComputeHash(Encoding.UTF8.GetBytes(matches[0].Groups["alias"].Value)));
                return String.Concat(shaString, "@", matches[0].Groups["domain"].Value);
            }

            return inString;
        }

    }
}
