using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace GZipTest
{
    public sealed class GZipCompressor : IDisposable
    {
        private readonly bool _disposed;
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(Environment.ProcessorCount);

        public void Dispose()
        {
            if (!_disposed)
            {
                _semaphore.Dispose();
            }
        }

        /// <summary>
        ///     Decompress file
        /// </summary>
        /// <param name="inputFile">input file path</param>
        /// <param name="outputFile">output file path</param>
        public void Decompress(string inputFile, string outputFile) => throw new NotImplementedException();

        /// <summary>
        ///     Compress file
        /// </summary>
        /// <param name="inputFile">input file path</param>
        /// <param name="outputFile">output file path</param>
        public void Compress(string inputFilePath, string outputFilePath)
        {
            using var factory = new GZipChunkFactory()
            {
                InputFile = inputFilePath,
                Operation = Operation.Compress
            };

            var chunks = factory.Create().ToList();

            var threads = new List<Thread>();
            chunks.ForEach(chunk =>
            {
                _semaphore.Wait();
                var thread = new Thread(() =>
                {
                    try
                    {
                        chunk.Process();
                    }
                    finally
                    {
                        _semaphore.Release();
                    }
                });
                thread.Start();
                threads.Add(thread);
            });
            threads.ForEach(thread => thread.Join());

            ConcatChunks(chunks, factory.HeaderSize, outputFilePath);
        }

        private void ConcatChunks(List<GZipChunk> chunks, int headerSize, string outputFile)
        {
            using var output = new FileStream(outputFile, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.SequentialScan);
            var header = new Span<byte>(new byte[headerSize]);
            BitConverter.TryWriteBytes(header.Slice(0, 4), chunks.Count);
            for (var i = 0; i < chunks.Count; i++)
            {
                BitConverter.TryWriteBytes(header.Slice(8 * i + 4, 8), chunks[i].Start);
            }
            output.Write(header);

            chunks.ForEach(chunk =>
            {
                using var input = new FileStream(chunk.FilePartPath, FileMode.Open, FileAccess.Read, FileShare.None, 4096, FileOptions.SequentialScan);
                input.CopyTo(output);
            });
        }
    }
}
