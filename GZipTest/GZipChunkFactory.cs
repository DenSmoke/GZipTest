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
            if (_fileSize == 0)
                throw new InvalidOperationException("File is empty");

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
            if (_fileSize <= 1024 * 1024)
            {
                yield return new GZipChunk(_mmf, Operation, 0, _fileSize);
            }
            else
            {
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
        }

        private IEnumerable<GZipChunk> CreateForDecompression()
        {
            _mmf?.Dispose();
            _mmf = MemoryMappedFile.CreateFromFile(InputFile, FileMode.Open, Path.GetFileName(InputFile), 0, MemoryMappedFileAccess.Read);

            int chunksCount;
            using var viewAccessor = _mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
            long[] gzipBlockIndices;
            try
            {
                chunksCount = viewAccessor.ReadInt32(_fileSize - 4);
                if (chunksCount < 0)
                    throw new InvalidOperationException("Wrong file format");
                gzipBlockIndices = new long[chunksCount];
                for (var i = 0; i < chunksCount; i++)
                {
                    long position;
                    long index;
                    if ((position = _fileSize - 8 * (chunksCount - i) - 4) < 0 || (index = viewAccessor.ReadInt64(position)) < 0)
                        throw new InvalidOperationException("Wrong file format");
                    gzipBlockIndices[i] = index;
                }
            }
            catch (InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new Exception($"File parse error: {ex.Message}", ex);
            }

            for (var i = 0; i < chunksCount; i++)
            {
                var start = gzipBlockIndices[i];
                var chunkSize = i + 1 < chunksCount
                    ? gzipBlockIndices[i + 1] - start
                    : _fileSize - start - (chunksCount * 8 + 4);
                yield return new GZipChunk(_mmf, Operation, start, chunkSize);
            }
        }
    }
}
