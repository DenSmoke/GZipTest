using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;

namespace GZipTest
{

    internal sealed class GZipChunk
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

        public void Process()
        {
            switch (Operation)
            {
                case Operation.Compress: Compress(); break;
                case Operation.Decompress: Decompress(); break;
                default: break;
            }
        }

        public int BufferSize { get; }

        public string InputFile { get; }

        public Operation Operation { get; }

        public long Start { get; }

        public long Length { get; }

        public GZipChunkState State { get; private set; }

        public Exception Error { get; private set; }

        public Stream GetResultStream() => State == GZipChunkState.Completed
            ? new FileStream(_tempFilePath, FileMode.Open, FileAccess.Read, FileShare.None, BufferSize, FileOptions.SequentialScan)
            : null;

        private void Compress()
        {
            State = GZipChunkState.Processing;

            var byteArray = ArrayPool<byte>.Shared.Rent(BufferSize);
            try
            {
                using var input = new FileStream(InputFile, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize, FileOptions.RandomAccess)
                {
                    Position = Start
                };
                _tempFilePath = Path.GetTempFileName();
                using var fs = new FileStream(_tempFilePath, FileMode.Open, FileAccess.Write, FileShare.None, BufferSize, FileOptions.SequentialScan);
                using var gz = new GZipStream(fs, CompressionMode.Compress);

                var bytesToRead = Length;
                var bytesRead = 0;
                var buffer = byteArray.AsSpan(0, BufferSize);
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

                State = GZipChunkState.Completed;
            }
            catch (Exception ex)
            {
                Error = ex;
                State = GZipChunkState.Failed;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(byteArray);
            }
        }

        private void Decompress() => throw new NotImplementedException();

    }
}
