using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace GZipTest
{
    public sealed class GZipChunkFactory : IDisposable
    {
        private static readonly PerformanceCounter _ramCounter = new PerformanceCounter("Memory", "Available Bytes");
        private bool _disposed;

        public GZipChunkFactory()
        {
            TempPath = Path.Combine(Path.GetTempPath(), $"GZipChunks_{Guid.NewGuid()}");
            Directory.CreateDirectory(TempPath);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                Directory.Delete(TempPath, true);
                _disposed = true;
            }
        }

        public string TempPath { get; }

        public string InputFile { get; set; }

        public Operation Operation { get; set; }

        public int HeaderSize { get; private set; }

        public int MaxBufferSize { get; private set; }

        public IEnumerable<GZipChunk> Create()
        {
            if (!File.Exists(InputFile))
                throw new InvalidOperationException("Input file does not exists");

            MaxBufferSize = (int)Math.Min(_ramCounter.NextValue() * 0.8f / Environment.ProcessorCount, int.MaxValue);
            if (MaxBufferSize < 1024 * 1024)
                throw new InsufficientMemoryException("Not enough memory");

            return Operation switch
            {
                Operation.Compress => CreateForCompression(),
                Operation.Decompress => CreateForDecompression(),
                _ => throw new InvalidOperationException("Unknown operation"),
            };
        }

        private IEnumerable<GZipChunk> CreateForCompression()
        {
            var fileSize = new FileInfo(InputFile).Length;
            var processors = Environment.ProcessorCount;
            var chunkSize = (int)Math.Min(Math.Round((double)fileSize / processors), int.MaxValue);
            var chunksCount = (int)(fileSize / chunkSize) + 1;
            HeaderSize = 4 + chunksCount * 8;

            long start = HeaderSize + 1;
            for (var i = 0; i < chunksCount - 1; i++)
            {
                var end = start + chunkSize;
                yield return new GZipChunk(i, start, end, this);
                start = end + 1;
            }
            yield return new GZipChunk(chunksCount, start, fileSize, this);
        }

        private IEnumerable<GZipChunk> CreateForDecompression() => throw new NotImplementedException();
    }
}
