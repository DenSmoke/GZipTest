using System;
using System.IO;
using System.IO.Compression;
using System.IO.MemoryMappedFiles;

namespace GZipTest
{

    internal sealed class GZipChunk
    {
        private readonly MemoryMappedFile _mmf;
        private string _tempFilePath;

        public GZipChunk(MemoryMappedFile input, Operation operation, long start, long length)
        {
            _mmf = input;
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

        public Operation Operation { get; }

        public long Start { get; }

        public long Length { get; }

        public GZipChunkState State { get; private set; }

        public Exception Error { get; private set; }

        public Stream GetResultStream() => State == GZipChunkState.Completed
            ? new FileStream(_tempFilePath, FileMode.Open, FileAccess.Read, FileShare.None, 4096, FileOptions.SequentialScan)
            : null;

        private void Compress()
        {
            State = GZipChunkState.Processing;
            try
            {
                using var input = _mmf.CreateViewStream(Start, Length, MemoryMappedFileAccess.Read);
                _tempFilePath = Path.GetTempFileName();
                using var output = new FileStream(_tempFilePath, FileMode.Open, FileAccess.Write, FileShare.None, 4096, FileOptions.SequentialScan);
                using var gz = new GZipStream(output, CompressionMode.Compress);
                input.CopyTo(gz);
                State = GZipChunkState.Completed;
            }
            catch (Exception ex)
            {
                Error = ex;
                State = GZipChunkState.Failed;
                if (_tempFilePath is object)
                    File.Delete(_tempFilePath);
            }
        }

        private void Decompress()
        {
            State = GZipChunkState.Processing;
            try
            {
                using var input = _mmf.CreateViewStream(Start, Length, MemoryMappedFileAccess.Read);
                using var gz = new GZipStream(input, CompressionMode.Decompress);
                _tempFilePath = Path.GetTempFileName();
                using var output = new FileStream(_tempFilePath, FileMode.Open, FileAccess.Write, FileShare.None, 4096, FileOptions.SequentialScan);
                gz.CopyTo(output);
                State = GZipChunkState.Completed;
            }
            catch (Exception ex)
            {
                Error = ex;
                State = GZipChunkState.Failed;
                if (_tempFilePath is object)
                    File.Delete(_tempFilePath);
            }
        }

    }
}
