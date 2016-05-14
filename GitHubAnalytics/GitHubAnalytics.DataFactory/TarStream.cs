using System;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace GitHubAnalytics.DataFactory
{
    public class TarStream : Stream
    {
        private readonly Stream _baseStream;
        private readonly BinaryReader _binaryReader;
        private long _currentFileLength;
        private long _currentFilePosition;

        public TarStream(Stream baseStream)
        {
            _baseStream = baseStream;

            _binaryReader = new BinaryReader(_baseStream);

        }

        public string CurrentFilename { get; private set; }


        public override bool CanRead => _baseStream.CanRead;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => _currentFileLength;

        public override long Position
        {
            get { return _currentFilePosition; }

            set { throw new NotImplementedException(); }
        }

        private bool ReadHeader(BinaryReader binaryReader)
        {
            if (!binaryReader.BaseStream.CanRead)
            {
                return false;
            }
            Debug.Assert(_currentFilePosition % 512 == 0);
            CurrentFilename = Encoding.ASCII.GetString(binaryReader.ReadBytes(100)).TrimEnd((char) (0));

            if (string.IsNullOrWhiteSpace(CurrentFilename))
            {
                return false;
            }

            binaryReader.ReadInt64(); // FileMode
            binaryReader.ReadInt64(); // Owner
            binaryReader.ReadInt64(); // Group

            var sizeByteArray = binaryReader.ReadBytes(12);

            if ((sizeByteArray[0] & 0x80) == 0)
            {
                var sizeString = Encoding.ASCII.GetString(sizeByteArray).TrimEnd((char) (0));
                _currentFileLength = Convert.ToInt64(sizeString, 8); // Size
            }
            else
            {
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(sizeByteArray, 4, 8);
                _currentFileLength = BitConverter.ToInt64(sizeByteArray, 4);
            }

            binaryReader.ReadBytes(12); // LastModificationTime
            binaryReader.ReadBytes(8); // Checksum
            binaryReader.ReadChar(); // FileType
            binaryReader.ReadBytes(100); // Name of linked file

            binaryReader.ReadBytes(255); // Rest of header

            _currentFilePosition = 0;

            return true;
        }

        public override void Close()
        {
            _binaryReader.Close();
            base.Close();
        }

        public bool NextFile()
        {
            // move to start of next file, returning false if the next file isn't available
            if (_currentFileLength > 0)
            {
                long remainingBytes;
                while ((remainingBytes = (_currentFileLength + (512 - (_currentFileLength%512)) - _currentFilePosition)) > 0)
                {
                    var countToRead = (int)Math.Min(remainingBytes, 1024*1024*8);
                    _binaryReader.ReadBytes(countToRead);
                    _currentFilePosition += countToRead;
                }

            }

            var isNextHeaderValid = ReadHeader(_binaryReader);
            return isNextHeaderValid;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var countToRead = (int) Math.Min(_currentFileLength - _currentFilePosition, count);

            var bytesRead = _baseStream.Read(buffer, offset, countToRead);
            _currentFilePosition += bytesRead;

            return bytesRead;
        }

        public override void Flush()
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
            throw new NotImplementedException();
        }
    }
}