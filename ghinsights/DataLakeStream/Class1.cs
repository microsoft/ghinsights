using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataLake.StoreUploader;

namespace DataLakeStream
{
    public class DataLakeStream : Stream
    {
        private DataLakeStoreFrontEndAdapter _dlFrontEnd;
        private string _streamPath;

        public DataLakeStream(DataLakeStoreFrontEndAdapter dlFrontEnd, string streamName)
        {
            _dlFrontEnd = dlFrontEnd;
            _streamPath = streamName;
            _dlFrontEnd.CreateStream(streamName, true, null, 0);
            
        }

        public override bool CanRead
        {
            get
            {
                throw new NotSupportedException();
            }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override long Length
        {
            get
            {
                throw new NotSupportedException();
            }
        }

        public override long Position
        {
            get
            {
                throw new NotSupportedException();
            }

            set
            {
                throw new NotSupportedException();
            }
        }

        public override void Flush()
        {
            //
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {

            _dlFrontEnd.AppendToStream(_streamPath, buffer, offset, count);

        }
        
    }

}
