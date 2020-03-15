using System;

namespace GZipTest
{

    public static class Program
    {
        /// <summary>
        ///     Entry point
        /// </summary>
        /// <param name="args">Command line arguments: compress/decompress input_file output_file</param>
        public static int Main(string[] args)
        {
            try
            {
                if (args?.Length != 3)
                    throw new InvalidOperationException("Invalid parameters");

                var operation = args[0];
                var inputFile = args[1];
                var outputFile = args[2];

                switch (operation)
                {
                    case "compress": GZipCompressor.Compress(inputFile, outputFile); break;
                    case "decompress": GZipCompressor.Decompress(inputFile, outputFile); break;
                    default: throw new InvalidOperationException("Invalid \"operation\" argument");
                }
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 1;
            }
        }
    }
}
