using System;
using System.IO;
using System.IO.Compression;
using System.IO.MemoryMappedFiles;
using System.Threading;

namespace GZipTest
{
    /// <summary>
    ///     Class for compression/decompression using separate <see cref="Thread"/>
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1001:Types that own disposable fields should be disposable", Justification = "<Pending>")]
    internal sealed class GZipChunk
    {
        private Thread _thread;
        private Stream _resultStream;

        /// <summary>
        ///     Create <see cref="GZipChunk"/> instance
        /// </summary>
        /// <param name="id">identifier of chunk</param>
        /// <param name="mode">compression mode</param>
        /// <param name="start">start position in input file</param>
        /// <param name="length">length in input file</param>
        public GZipChunk(int id, CompressionMode mode, long start, long length)
        {
            Id = id;
            Mode = mode;
            Start = start;
            Length = length;
        }

        /// <summary>
        ///     Finalizer for disposing result stream
        /// </summary>
        ~GZipChunk()
        {
            if (_resultStream is object)
            {
                _resultStream.Dispose();
                _resultStream = null;
            }
        }

        /// <summary>
        ///     Chunk identifier
        /// </summary>
        public int Id { get; }

        /// <summary>
        ///     Compression mode
        /// </summary>
        public CompressionMode Mode { get; }

        /// <summary>
        ///     Start position in input file
        /// </summary>
        public long Start { get; }

        /// <summary>
        ///     Length in input file
        /// </summary>
        public long Length { get; }

        /// <summary>
        ///     State of chunk
        /// </summary>
        public GZipChunkState State { get; private set; }

        /// <summary>
        ///     Contains exception if <see cref="State"/> is <see cref="GZipChunkState.Failed"/>
        /// </summary>
        public Exception Error { get; private set; }

        /// <summary>
        ///     Create <see cref="Thread"/> and start processing of chunk
        /// </summary>
        /// <param name="inputFile">input file wrapped in <see cref="MemoryMappedFile"/></param>
        /// <exception cref="ThreadStateException"/>
        /// <exception cref="OutOfMemoryException"/>
        /// <exception cref="ArgumentNullException"/>
        public void StartProcessing(MemoryMappedFile inputFile)
        {
            if (inputFile is null)
                throw new ArgumentNullException(nameof(inputFile));

            _thread = new Thread(() => Process(inputFile));
            _thread.Start();
        }

        /// <summary>
        ///     Write result of operation to output stream. Blocks caller thread, if operation is not completed
        /// </summary>
        /// <param name="output">output stream</param>
        /// <exception cref="Exception"/>
        /// <exception cref="InvalidOperationException"/>
        /// <exception cref="ThreadStateException"/>
        /// <exception cref="ThreadInterruptedException"/>
        /// <exception cref="ObjectDisposedException"/>
        /// <exception cref="NotSupportedException"/>
        /// <exception cref="ArgumentNullException"/>
        /// <exception cref="IOException"/>
        public void FlushChunk(Stream output)
        {
            if (output is null)
                throw new ArgumentNullException(nameof(output));

            if (_thread is null)
                throw new InvalidOperationException("Processing should be started before flushing");

            if (_thread.IsAlive)
                _thread.Join();

            if (State == GZipChunkState.Completed && _resultStream is object)
                using (_resultStream)
                    _resultStream.CopyTo(output);
            else
                throw new Exception($"Error occured while processing a chunk: {Error?.Message}", Error);
        }

        /// <summary>
        ///     Process chunk
        /// </summary>
        /// <param name="inputFile">input file wrapped in <see cref="MemoryMappedFile"/></param>
        private void Process(MemoryMappedFile inputFile)
        {
            State = GZipChunkState.Processing;
            try
            {
                if (inputFile is null)
                    throw new ArgumentNullException(nameof(inputFile));

                _resultStream = new FileStream(Path.GetTempFileName(), FileMode.Open, FileAccess.ReadWrite,
                                               FileShare.None, 4096, FileOptions.DeleteOnClose);

                using (var input = inputFile.CreateViewStream(Start, Length, MemoryMappedFileAccess.Read))
                {
                    switch (Mode)
                    {
                        case CompressionMode.Compress:
                            using (var gz = new GZipStream(_resultStream, Mode, true))
                                input.CopyTo(gz);
                            break;
                        case CompressionMode.Decompress:
                            using (var gz = new GZipStream(input, Mode, true))
                                gz.CopyTo(_resultStream);
                            break;
                        default: throw new InvalidOperationException("Unknown operation type");
                    }
                }
                _resultStream.Position = 0;
                State = GZipChunkState.Completed;
            }
            catch (Exception ex)
            {
                Error = ex;
                State = GZipChunkState.Failed;
                if (_resultStream is object)
                {
                    _resultStream.Dispose();
                    _resultStream = null;
                }
            }
        }
    }
}
