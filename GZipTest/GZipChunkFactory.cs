using System;
using System.Collections.Generic;
using System.IO;

namespace GZipTest
{
    internal sealed class GZipChunkFactory
    {

        public GZipChunkFactory()
        {
            BufferSize = 8192;
            ChunksCount = Environment.ProcessorCount;
        }

        public int BufferSize { get; set; }

        public string InputFile { get; set; }

        public Operation Operation { get; set; }

        public int ChunksCount { get; set; }

        public IEnumerable<GZipChunk> Create()
        {
            if (!File.Exists(InputFile))
                throw new InvalidOperationException("Input file does not exists");
            if (BufferSize < 1)
                throw new InvalidOperationException("Invalid buffer size");

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
            var div = fileSize / ChunksCount;
            var rem = fileSize % ChunksCount;

            long start = 0;
            for (var i = 0; i < ChunksCount; i++)
            {
                var chunkSize = div;
                if (rem > 0)
                {
                    chunkSize++;
                    rem--;
                }
                yield return new GZipChunk(BufferSize, InputFile, Operation, start, chunkSize);
                start += chunkSize;
            }
        }

        private IEnumerable<GZipChunk> CreateForDecompression() => throw new NotImplementedException();
    }
}
