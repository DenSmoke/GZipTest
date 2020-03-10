using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;

namespace GZipTest
{

    public sealed class GZipChunk
    {
        private readonly GZipChunkFactory _factory;

        internal GZipChunk(int number, long start, long end, GZipChunkFactory factory)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            Number = number;
            Start = start;
            End = end;
        }

        public void Process()
        {
            switch (_factory.Operation)
            {
                case Operation.Compress: Compress(); break;
                case Operation.Decompress: Decompress(); break;
                default: break;
            }
        }

        public int Number { get; }

        public long Start { get; }

        public long End { get; }

        public string FilePartPath { get; private set; }

        private void Compress()
        {
            FilePartPath = Path.Combine(_factory.TempPath, $"part_{Number}.gz");

            using var input = new FileStream(_factory.InputFile, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan)
            {
                Position = Start
            };
            using var output = new FileStream(FilePartPath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.SequentialScan);
            using var gz = new GZipStream(output, CompressionMode.Compress);

            var pool = ArrayPool<byte>.Shared;
            var bytesToRead = (int)(End - Start);
            var bufferSize = Math.Min(_factory.MaxBufferSize, bytesToRead);
            var buffer = pool.Rent(bufferSize);

            try
            {
                do
                {
                    var bytesRead = 0;
                    var bytesToReadToBuffer = Math.Min(bytesToRead, bufferSize);
                    do
                    {
                        bytesRead += input.Read(buffer, bytesRead, bytesToReadToBuffer);
                        bytesToReadToBuffer -= bytesRead;
                    }
                    while (bytesToReadToBuffer > 0);

                    gz.Write(buffer, 0, bytesRead);

                    bytesToRead -= bytesRead;
                }
                while (bytesToRead > 0);
            }
            finally
            {
                pool.Return(buffer);
            }
        }

        private void Decompress() => throw new NotImplementedException();

    }
}
