using Microsoft.VisualStudio.TestTools.UnitTesting;
using GitHubAnalytics.USql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GitHubAnalytics.USql.Tests
{
    [TestClass()]
    public class UtilityTests
    {
        [TestMethod()]
        public void HashEmailTest()
        {
            var testInput = "test@whoever.blah";

            var result = Utility.HashEmail(testInput);

            Assert.AreEqual("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08@whoever.blah", result);
        }

        [TestMethod()]
        public void HashEmailTestWithWhitespace()
        {
            var testInput = "test@whoever.blah ";

            var result = Utility.HashEmail(testInput);

            Assert.AreEqual("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08@whoever.blah", result);
        }
        [TestMethod()]
        public void HashEmailTestWithWhitespaceInAlias()
        {
            var testInput = "tes t@whoever.blah";

            var result = Utility.HashEmail(testInput);

            Assert.AreEqual("6f01e737c4699443d5128a6d8f2a405c07329c9bf35c776b4d69c136bc728d34@whoever.blah", result);
        }
        [TestMethod()]
        public void HashEmailTestWithPeriod()
        {
            var testInput = "te.st@whoever.blah";

            var result = Utility.HashEmail(testInput);

            Assert.AreEqual("616bd82408817bbb6da4ab1ce6bfc5176691dbefb7d6c4f2e6e9d0db3b428889@whoever.blah", result);
        }

        [TestMethod()]
        public void HashEmailTestWithNull()
        {
            string testInput = null;

            var result = Utility.HashEmail(testInput);

            Assert.IsNull(result);
        }
    }
}