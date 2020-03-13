using System;
using System.IO;
using System.Linq;
using System.Threading;

namespace GZipTest
{
    internal sealed class GZipCompressor
    {

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
            var factory = new GZipChunkFactory
            {
                InputFile = inputFilePath,
                Operation = Operation.Compress,
                BufferSize = 8192
            };

            var chunks = factory.Create().ToList();
            try
            {
                var threads = chunks.Select(chunk =>
                {
                    var thread = new Thread(chunk.Process);
                    thread.Start();
                    return thread;
                }).ToList();

                using var output = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 131072, FileOptions.SequentialScan);
                for (var i = 0; i < factory.ChunksCount; i++)
                {
                    threads[i].Join();
                    chunks[i].Result.CopyTo(output);
                }
            }
            finally
            {
                chunks.ForEach(chunk => chunk.Dispose());
            }
        }
    }
}
