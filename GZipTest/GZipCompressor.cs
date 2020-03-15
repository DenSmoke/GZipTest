using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace GZipTest
{
    public static class GZipCompressor
    {

        /// <summary>
        ///     Decompress file
        /// </summary>
        /// <param name="inputFile">input file path</param>
        /// <param name="outputFile">output file path</param>
        public static void Decompress(string inputFile, string outputFile, int bufferSize = 8192) => throw new NotImplementedException();

        /// <summary>
        ///     Compress file
        /// </summary>
        /// <param name="inputFile">input file path</param>
        /// <param name="outputFile">output file path</param>
        public static void Compress(string inputFilePath, string outputFilePath, int bufferSize = 8192)
        {
            var factory = new GZipChunkFactory
            {
                InputFile = inputFilePath,
                Operation = Operation.Compress,
                BufferSize = bufferSize
            };

            using var output = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize, FileOptions.SequentialScan);

            var chunks = factory.Create().ToList();
            var threads = new List<Thread>(chunks.Count);

            chunks.ForEach(chunk =>
            {
                var thread = new Thread(chunk.Process);
                thread.Start();
                threads.Add(thread);
            });

            for (var i = 0; i < chunks.Count; i++)
            {
                threads[i].Join();

                var chunk = chunks[i];
                if (chunk.State == GZipChunkState.Completed)
                {
                    using var resultStream = chunk.GetResultStream();
                    resultStream.CopyTo(output);
                }
                else
                    throw new Exception($"Error occured while processing a chunk: {chunk.Error?.Message}", chunk.Error);
            }
        }
    }
}
