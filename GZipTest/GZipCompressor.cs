using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Threading;

namespace GZipTest
{
    /// <summary>
    ///     Class for multithreaded compression/decompression using <see cref="GZipStream"/>
    /// </summary>
    public class GZipCompressor
    {
        private long _fileSize;

        /// <summary>
        ///     Compress file
        /// </summary>
        /// <param name="inputFilePath">input file path</param>
        /// <param name="outputFilePath">output file path</param>
        /// <exception cref="Exception"/>
        /// <exception cref="ArgumentNullException"/>
        /// <exception cref="System.Security.SecurityException"/>
        /// <exception cref="ArgumentException"/>
        /// <exception cref="UnauthorizedAccessException"/>
        /// <exception cref="PathTooLongException"/>
        /// <exception cref="NotSupportedException"/>
        /// <exception cref="InvalidOperationException"/>
        /// <exception cref="IOException"/>
        /// <exception cref="FileNotFoundException"/>
        /// <exception cref="ArgumentOutOfRangeException"/>
        /// <exception cref="DirectoryNotFoundException"/>
        /// <exception cref="ThreadStateException"/>
        /// <exception cref="OutOfMemoryException"/>
        /// <exception cref="ThreadInterruptedException"/>
        /// <exception cref="ObjectDisposedException"/>
        /// <exception cref="OverflowException"/>
        public void Compress(string inputFilePath, string outputFilePath) =>
            Process(inputFilePath, outputFilePath, CompressionMode.Compress);

        /// <summary>
        ///     Decompress file
        /// </summary>
        /// <param name="inputFilePath">input file path</param>
        /// <param name="outputFilePath">output file path</param>
        /// <exception cref="Exception"/>
        /// <exception cref="ArgumentNullException"/>
        /// <exception cref="System.Security.SecurityException"/>
        /// <exception cref="ArgumentException"/>
        /// <exception cref="UnauthorizedAccessException"/>
        /// <exception cref="PathTooLongException"/>
        /// <exception cref="NotSupportedException"/>
        /// <exception cref="InvalidOperationException"/>
        /// <exception cref="IOException"/>
        /// <exception cref="FileNotFoundException"/>
        /// <exception cref="ArgumentOutOfRangeException"/>
        /// <exception cref="DirectoryNotFoundException"/>
        /// <exception cref="ThreadStateException"/>
        /// <exception cref="OutOfMemoryException"/>
        /// <exception cref="ThreadInterruptedException"/>
        /// <exception cref="ObjectDisposedException"/>
        /// <exception cref="OverflowException"/>
        public void Decompress(string inputFilePath, string outputFilePath) =>
            Process(inputFilePath, outputFilePath, CompressionMode.Decompress);

        /// <summary>
        ///     Generate chunks for file compression
        /// </summary>
        /// <returns></returns>
        private IEnumerable<GZipChunk> CreateForCompression()
        {
            if (_fileSize <= 1024 * 1024)
            {
                yield return new GZipChunk(CompressionMode.Compress, 0, _fileSize);
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
                    yield return new GZipChunk(CompressionMode.Compress, start, chunkSize);
                    start += chunkSize;
                }
            }
        }

        /// <summary>
        ///     Generate chunks for file decompression
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        /// <exception cref="Exception"/>
        /// <exception cref="ArgumentOutOfRangeException"/>
        /// <exception cref="UnauthorizedAccessException"/>
        /// <exception cref="IOException"/>
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
                yield return new GZipChunk(CompressionMode.Decompress, start, chunkSize);
            }
        }

        /// <summary>
        ///     Add footer containing start positions of chunks and their count to output stream
        /// </summary>
        /// <param name="output">output stream</param>
        /// <param name="chunkPointers">start positions of chunks</param>
        /// <exception cref="ArgumentNullException"/>
        /// <exception cref="ArgumentException"/>
        /// <exception cref="OverflowException"/>
        /// <exception cref="IOException"/>
        /// <exception cref="ObjectDisposedException"/>
        private static void AddFooter(Stream output, long[] chunkPointers)
        {
            if (output is null)
                throw new ArgumentNullException(nameof(output));
            if (chunkPointers is null)
                throw new ArgumentNullException(nameof(chunkPointers));

            using var bw = new BinaryWriter(output);
            for (var i = 0; i < chunkPointers.Length; i++)
                bw.Write(chunkPointers[i]);
            bw.Write(chunkPointers.Length);
        }

        /// <summary>
        ///     Compress or decompress input file to output file depending on <see cref="CompressionMode"/>
        /// </summary>
        /// <param name="inputFilePath">path to input file</param>
        /// <param name="outputFilePath">path to output file</param>
        /// <param name="mode">compression mode</param>
        /// <exception cref="Exception"/>
        /// <exception cref="ArgumentNullException"/>
        /// <exception cref="System.Security.SecurityException"/>
        /// <exception cref="ArgumentException"/>
        /// <exception cref="UnauthorizedAccessException"/>
        /// <exception cref="PathTooLongException"/>
        /// <exception cref="NotSupportedException"/>
        /// <exception cref="InvalidOperationException"/>
        /// <exception cref="IOException"/>
        /// <exception cref="FileNotFoundException"/>
        /// <exception cref="ArgumentOutOfRangeException"/>
        /// <exception cref="DirectoryNotFoundException"/>
        /// <exception cref="ThreadStateException"/>
        /// <exception cref="OutOfMemoryException"/>
        /// <exception cref="ThreadInterruptedException"/>
        /// <exception cref="ObjectDisposedException"/>
        /// <exception cref="OverflowException"/>
        private void Process(string inputFilePath, string outputFilePath, CompressionMode mode)
        {
            var fileInfo = new FileInfo(inputFilePath);
            if (!fileInfo.Exists)
                throw new FileNotFoundException("Input file does not exists");

            _fileSize = fileInfo.Length;
            if (_fileSize == 0)
                throw new InvalidOperationException("File is empty");

            using var input = MemoryMappedFile.CreateFromFile(inputFilePath, FileMode.Open, Path.GetFileName(inputFilePath), 0, MemoryMappedFileAccess.Read);
            using var output = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.SequentialScan);

            var chunks = (mode switch
            {
                CompressionMode.Compress => CreateForCompression(),
                CompressionMode.Decompress => CreateForDecompression(input),
                _ => throw new InvalidOperationException("Unknown operation mode")
            }).ToArray();
            Console.WriteLine("{0} chunks generated", chunks.Length);

            for (var i = 0; i < chunks.Length; i++)
                chunks[i].StartProcessing(input);
            Console.WriteLine("Chunks processing started");

            switch (mode)
            {
                case CompressionMode.Compress:
                    var chunkPointers = new long[chunks.Length];
                    for (var i = 0; i < chunks.Length; i++)
                    {
                        var chunk = chunks[i];
                        chunkPointers[i] = output.Position;
                        chunk.FlushChunk(output);
                        Console.WriteLine("Written chunk {0} of {1}", i + 1, chunks.Length);
                    }
                    AddFooter(output, chunkPointers);
                    return;

                case CompressionMode.Decompress:
                    for (var i = 0; i < chunks.Length; i++)
                    {
                        var chunk = chunks[i];
                        chunk.FlushChunk(output);
                        Console.WriteLine("Written chunk {0} of {1}", i + 1, chunks.Length);
                    }
                    return;

                default:
                    throw new InvalidOperationException("Unknown operation mode");
            }
        }
    }
}
