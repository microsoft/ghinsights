using System;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace GHInsights.DataFactory
{
    public class TarStream : Stream
    {
        private readonly Stream _baseStream;
        private readonly BinaryReader _binaryReader;
        private long _currentFileLength;
        private long _currentFilePosition;
        private bool _validHeader;

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

        private int ReadHeader()
        {
            _validHeader = false;
            var bytesRead = 0;
            try
            {
                if (!_binaryReader.BaseStream.CanRead)
                {
                    return bytesRead;
                }
                Debug.Assert(_currentFilePosition%512 == 0);
                CurrentFilename = Encoding.ASCII.GetString(_binaryReader.ReadBytes(100)).TrimEnd((char) (0));
                
                if (string.IsNullOrWhiteSpace(CurrentFilename))
                {
                    return bytesRead;
                }
                bytesRead += 100;

                _binaryReader.ReadInt64(); // FileMode
                bytesRead += 8;
                _binaryReader.ReadInt64(); // Owner
                bytesRead += 8;
                _binaryReader.ReadInt64(); // Group
                bytesRead += 8;

                var sizeByteArray = _binaryReader.ReadBytes(12);
                bytesRead += 12;

                if ((sizeByteArray[0] & 0x80) == 0)
                {
                    var sizeString = Encoding.ASCII.GetString(sizeByteArray).TrimEnd((char) (0));
                    _currentFileLength = Convert.ToInt64(sizeString, 8); // Size
                } else
                {
                    if (BitConverter.IsLittleEndian)
                        Array.Reverse(sizeByteArray, 4, 8);
                    _currentFileLength = BitConverter.ToInt64(sizeByteArray, 4);
                }

                _binaryReader.ReadBytes(12); // LastModificationTime
                bytesRead += 12;
                _binaryReader.ReadBytes(8); // Checksum
                bytesRead += 8;
                _binaryReader.ReadChar(); // FileType
                bytesRead += 1;
                _binaryReader.ReadBytes(100); // Name of linked file
                bytesRead += 100;


                _currentFilePosition = 0;

            } catch (System.FormatException)
            {
                return bytesRead;
            } catch (System.ArgumentOutOfRangeException)
            {
                return bytesRead;
            } finally
            {
                var restOfBlock = 512 - bytesRead;
                _binaryReader.ReadBytes(restOfBlock);
            }
            _validHeader = true;
            return bytesRead;
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

            int headerReadCount = 0;
            while (ReadHeader() > 0 && !_validHeader)
            {
                if (headerReadCount++ > 5)
                {
                    throw new FormatException("Unable to find header for next file");
                }
            };

            return _validHeader;
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