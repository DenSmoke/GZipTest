using System;
using System.IO;

namespace GZipTest
{

    internal class Program
    {
        /// <summary>
        ///     Entry point
        /// </summary>
        /// <param name="args">Command line arguments: compress/decompress input_file output_file</param>
        private static int Main(string[] args)
        {
            try
            {
                if (args.Length < 3)
                    throw new InvalidOperationException("Not enough arguments");

                var operation = args[0];
                var inputFile = args[1];
                var outputFile = args[2];

                if (!File.Exists(inputFile))
                    throw new FileNotFoundException("Input file is not found");

                var compressor = new GZipCompressor();
                switch (operation)
                {
                    case "compress": compressor.Compress(inputFile, outputFile); break;
                    case "decompress": compressor.Decompress(inputFile, outputFile); break;
                    default: throw new InvalidOperationException("Invalid \"operation\" argument");
                }
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return 1;
            }
        }
    }
}
