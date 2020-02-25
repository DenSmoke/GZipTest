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
        private static void Main(string[] args)
        {
            var operation = args[0];
            var inputFile = args[1];
            var outputFile = args[2];

            try
            {
                if (!File.Exists(inputFile))
                    throw new FileNotFoundException("Input file is not found");

                switch (operation)
                {
                    case Operations.Compress: GZipCompressor.Compress(inputFile, outputFile); break;
                    case Operations.Decompress: GZipCompressor.Decompress(inputFile, outputFile); break;
                    default: throw new InvalidOperationException("Invalid \"operation\" argument");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                Console.WriteLine("Press any key to exit");
                Console.Read();
            }
        }
    }
}
