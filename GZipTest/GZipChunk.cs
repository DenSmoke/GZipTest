using System;
using System.IO;
using System.IO.Compression;
using System.IO.MemoryMappedFiles;
using System.Threading;

namespace GZipTest
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1001:Types that own disposable fields should be disposable", Justification = "<Pending>")]
    internal sealed class GZipChunk
    {
        private Thread _thread;
        private Stream _resultStream;

        public GZipChunk(int id, CompressionMode mode, long start, long length)
        {
            Id = id;
            Mode = mode;
            Start = start;
            Length = length;
        }

        ~GZipChunk()
        {
            if (_resultStream is object)
            {
                _resultStream.Dispose();
                _resultStream = null;
            }
        }

        public int Id { get; }

        public CompressionMode Mode { get; }

        public long Start { get; }

        public long Length { get; }

        public GZipChunkState State { get; private set; }

        public Exception Error { get; private set; }

        public void StartProcessing(MemoryMappedFile inputFile)
        {
            _thread = new Thread(() => Process(inputFile));
            _thread.Start();
        }

        public void FlushChunk(Stream output)
        {
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

        private void Process(MemoryMappedFile inputFile)
        {
            State = GZipChunkState.Processing;
            try
            {
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
