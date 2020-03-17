namespace GZipTest
{
    /// <summary>
    ///     Enum representing state of GZipChunk
    /// </summary>
    internal enum GZipChunkState
    {
        New,
        Processing,
        Completed,
        Failed
    }
}
