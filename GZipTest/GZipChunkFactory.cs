using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace GZipTest
{
    internal sealed class GZipChunkFactory : IDisposable
    {
        private MemoryMappedFile _mmf;
        private long _fileSize;

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
            _mmf?.Dispose();
            _mmf = MemoryMappedFile.CreateFromFile(InputFile, FileMode.Open, Path.GetFileName(InputFile), 0, MemoryMappedFileAccess.Read);

            var processors = Environment.ProcessorCount;
            var div = _fileSize / processors;
            var rem = _fileSize % processors;
            long start = 0;
            for (var i = 0; i < processors; i++)
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
            _mmf?.Dispose();
            _mmf = MemoryMappedFile.CreateFromFile(InputFile, FileMode.Open, Path.GetFileName(InputFile), 0, MemoryMappedFileAccess.Read);

            using var input = _mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
            var chunksCount = input.ReadInt32(_fileSize - 4);
            var gzipBlockIndices = new List<long>(chunksCount);
            for (var i = 0; i < chunksCount; i++)
                gzipBlockIndices.Add(input.ReadInt64(_fileSize - 8 * (chunksCount - i) - 4));

            for (var i = 0; i < gzipBlockIndices.Count; i++)
            {
                var start = gzipBlockIndices[i];
                var chunkSize = i + 1 < gzipBlockIndices.Count
                    ? gzipBlockIndices[i + 1] - start
                    : _fileSize - start - (chunksCount * 8 + 4);
                yield return new GZipChunk(_mmf, Operation, start, chunkSize);
            }
        }
    }
}
