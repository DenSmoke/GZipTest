using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.IO.MemoryMappedFiles;
using System.Linq;

namespace GZipTest
{
    public class GZipCompressor
    {
        private long _fileSize;

        /// <summary>
        ///     Compress file
        /// </summary>
        /// <param name="inputFilePath">input file path</param>
        /// <param name="outputFilePath">output file path</param>
        public void Compress(string inputFilePath, string outputFilePath) =>
            Process(inputFilePath, outputFilePath, CompressionMode.Compress);

        /// <summary>
        ///     Decompress file
        /// </summary>
        /// <param name="inputFilePath">input file path</param>
        /// <param name="outputFilePath">output file path</param>
        public void Decompress(string inputFilePath, string outputFilePath) =>
            Process(inputFilePath, outputFilePath, CompressionMode.Decompress);

        private IEnumerable<GZipChunk> CreateForCompression()
        {
            if (_fileSize <= 1024 * 1024)
            {
                yield return new GZipChunk(1, CompressionMode.Compress, 0, _fileSize);
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
                    yield return new GZipChunk(i + 1, CompressionMode.Compress, start, chunkSize);
                    start += chunkSize;
                }
            }
        }

        private IEnumerable<GZipChunk> CreateForDecompression(MemoryMappedFile input)
        {
            int chunksCount;
            using var viewAccessor = input.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
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
                yield return new GZipChunk(i + 1, CompressionMode.Decompress, start, chunkSize);
            }
        }

        private static void AddFooter(FileStream output, long[] chunkPointers)
        {
            using var bw = new BinaryWriter(output);
            for (var i = 0; i < chunkPointers.Length; i++)
                bw.Write(chunkPointers[i]);
            bw.Write(chunkPointers.Length);
        }

        private void Process(string inputFilePath, string outputFilePath, CompressionMode mode)
        {
            var fileInfo = new FileInfo(inputFilePath);
            if (!fileInfo.Exists)
                throw new InvalidOperationException("Input file does not exists");

            _fileSize = fileInfo.Length;
            if (_fileSize == 0)
                throw new InvalidOperationException("File is empty");

            using var input = MemoryMappedFile.CreateFromFile(inputFilePath, FileMode.Open, Path.GetFileName(inputFilePath), 0, MemoryMappedFileAccess.Read);
            using var output = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.SequentialScan);

            Console.WriteLine("Chunks generation start");

            var chunks = (mode switch
            {
                CompressionMode.Compress => CreateForCompression(),
                CompressionMode.Decompress => CreateForDecompression(input),
                _ => throw new InvalidOperationException("Unknown operation mode")
            }).ToArray();

            Console.WriteLine("{0} chunks generated", chunks.Length);
            Console.WriteLine("Chunks processing start");

            for (var i = 0; i < chunks.Length; i++)
                chunks[i].StartProcessing(input);

            Console.WriteLine("Chunks flushing start");

            switch (mode)
            {
                case CompressionMode.Compress:
                    var chunkPointers = new long[chunks.Length];
                    for (var i = 0; i < chunks.Length; i++)
                    {
                        var chunk = chunks[i];
                        Console.WriteLine("Writing chunk {0}", chunk.Id);
                        chunkPointers[i] = output.Position;
                        chunk.FlushChunk(output);
                    }
                    AddFooter(output, chunkPointers);
                    return;

                case CompressionMode.Decompress:
                    for (var i = 0; i < chunks.Length; i++)
                    {
                        var chunk = chunks[i];
                        Console.WriteLine("Writing chunk {0}", chunk.Id);
                        chunk.FlushChunk(output);
                    }
                    return;

                default:
                    throw new InvalidOperationException("Unknown operation mode");
            }
        }
    }
}
