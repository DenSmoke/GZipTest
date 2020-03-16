using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace GZipTest
{
    internal sealed class GZipChunkFactory : IDisposable
    {
        private MemoryMappedFile _mmf;
        private const int GZIP_HEADER_SIZE = 10;
        private long _fileSize;

        public GZipChunkFactory() => ChunksCount = Environment.ProcessorCount;

        public void Dispose()
        {
            if (_mmf is object)
            {
                _mmf.Dispose();
                _mmf = null;
            }
        }

        public string InputFile { get; set; }

        public Operation Operation { get; set; }

        public int ChunksCount { get; set; }

        public IEnumerable<GZipChunk> Create()
        {
            var fileInfo = new FileInfo(InputFile);
            if (!fileInfo.Exists)
                throw new InvalidOperationException("Input file does not exists");

            _fileSize = fileInfo.Length;
            return Operation switch
            {
                Operation.Compress => CreateForCompression(),
                Operation.Decompress => CreateForDecompression(),
                _ => throw new InvalidOperationException("Unknown operation"),
            };
        }

        private IEnumerable<GZipChunk> CreateForCompression()
        {
            Dispose();
            _mmf = MemoryMappedFile.CreateFromFile(InputFile, FileMode.Open, Path.GetFileName(InputFile), 0, MemoryMappedFileAccess.Read);

            var div = _fileSize / ChunksCount;
            var rem = _fileSize % ChunksCount;
            long start = 0;
            for (var i = 0; i < ChunksCount; i++)
            {
                var chunkSize = div;
                if (rem > 0)
                {
                    chunkSize++;
                    rem--;
                }
                yield return new GZipChunk(_mmf, Operation, start, chunkSize);
                start += chunkSize;
            }
        }

        private IEnumerable<GZipChunk> CreateForDecompression()
        {
            Dispose();
            _mmf = MemoryMappedFile.CreateFromFile(InputFile, FileMode.Open, Path.GetFileName(InputFile), 0, MemoryMappedFileAccess.Read);

            var blockIndices = GetBlockIndices();
            for (var i = 0; i < blockIndices.Count; i++)
            {
                var start = blockIndices[i];
                var chunkSize = i + 1 < blockIndices.Count ? blockIndices[i + 1] - start : _fileSize - start;
                yield return new GZipChunk(_mmf, Operation, start, chunkSize);
            }
        }

        public List<long> GetBlockIndices()
        {
            using var input = _mmf.CreateViewStream(0, 0, MemoryMappedFileAccess.Read);

            var header = new byte[GZIP_HEADER_SIZE];
            long bytesRead = 0;
            var bytesToRead = GZIP_HEADER_SIZE;
            do
            {
                var n = input.Read(header, (int)bytesRead, bytesToRead);
                bytesRead += n;
                bytesToRead -= n;
            }
            while (bytesToRead > 0);

            var blockIndices = new List<long>() { 0 };
            var matchCount = 0;

            var byteArray = ArrayPool<byte>.Shared.Rent(100 * 1024 * 1024);
            try
            {
                while (true)
                {
                    var n = input.Read(byteArray);
                    if (n == 0)
                        break;

                    bytesRead += n;
                    for (var i = 0; i < n; i++)
                    {
                        if (byteArray[i] == header[matchCount])
                        {
                            matchCount++;
                            if (matchCount == GZIP_HEADER_SIZE)
                            {
                                blockIndices.Add(bytesRead - (n - i) - (GZIP_HEADER_SIZE - 1));
                                matchCount = 0;
                            }
                        }
                        else
                            matchCount = 0;
                    }
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(byteArray);
            }
            return blockIndices;
        }
    }
}
