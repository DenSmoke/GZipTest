using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using System.Runtime;

namespace GZipTest
{

    internal sealed class GZipChunk : IDisposable
    {
        private string _tempFilePath;

        public GZipChunk(int bufferSize, string inputFile, Operation operation, long start, long length)
        {
            BufferSize = bufferSize;
            InputFile = inputFile ?? throw new ArgumentNullException(nameof(inputFile));
            Operation = operation;
            Start = start;
            Length = length;
        }

        public void Dispose()
        {
            if (Result is object)
            {
                Result.Dispose();
                Result = null;
            }
            if (_tempFilePath is object)
            {
                File.Delete(_tempFilePath);
            }
        }

        public void Process()
        {
            switch (Operation)
            {
                case Operation.Compress: Compress(); break;
                case Operation.Decompress: Decompress(); break;
                default: break;
            }
        }

        public int BufferSize { get; set; }

        public string InputFile { get; set; }

        public Operation Operation { get; set; }

        public long Start { get; }

        public long Length { get; }

        public Stream Result { get; private set; }

        private void Compress()
        {
            using var input = new FileStream(InputFile, FileMode.Open, FileAccess.Read, FileShare.Read, 131072, FileOptions.RandomAccess)
            {
                Position = Start
            };

            var bytesToRead = Length;
            var pool = ArrayPool<byte>.Shared;
            var byteArray = pool.Rent(BufferSize);
            var buffer = byteArray.AsSpan(0, BufferSize);
            MemoryFailPoint mfp = null;
            try
            {
                try
                {
                    mfp = new MemoryFailPoint((int)(Length / 1024 / 1024));
                    Result = new MemoryStream();
                }
                catch (InsufficientMemoryException)
                {
                    _tempFilePath = Path.GetTempFileName();
                    Result = new FileStream(_tempFilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 131072, FileOptions.RandomAccess);
                }

                using var gz = new GZipStream(Result, CompressionMode.Compress, true);
                var bytesRead = 0;
                do
                {
                    var n = bytesToRead >= BufferSize
                        ? input.Read(buffer)
                        : input.Read(buffer.Slice(0, (int)bytesToRead));
                    if (n == BufferSize)
                        gz.Write(buffer);
                    else
                        gz.Write(buffer.Slice(0, n));
                    bytesRead += n;
                    bytesToRead -= n;
                } while (bytesToRead > 0);
            }
            finally
            {
                pool.Return(byteArray);
                if (mfp is object)
                    mfp.Dispose();
                Result.Position = 0;
            }
        }

        private void Decompress() => throw new NotImplementedException();

    }
}
