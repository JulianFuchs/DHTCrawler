using System.IO;

namespace BencodeLibrary
{
    public interface IBencodingType
    {
        /// <summary>
        /// Encodes the current object onto the specified binary writer.
        /// </summary>
        /// <param name="writer">The writer to write to - must not be null</param>
        void Encode(BinaryWriter writer);
    }
}