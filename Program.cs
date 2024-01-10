using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace CameronAavik.OneBRC;

public class Program
{
    public static void Main(string[] args)
    {
        var sw = new Stopwatch();
        sw.Start();
        new OneBrcSolver().Run(args[0]);
        Console.WriteLine($"Duration: {sw.Elapsed}");
    }
}

/**
 * Mutable struct that represents the entries in my linear-probing hash map. With First4Bytes, Last4Bytes, and 
 * Next8Bytes, it can store city names of up to length 16. If the length is more than 16, then Next8Bytes will instead 
 * store a nuint which represents a pointer to the rest of the city name.
 */
public struct HashEntry()
{
    public uint First4Bytes;
    public uint Last4Bytes;
    public int Length;
    public int Count = 1;
    public int Min;
    public int Max;
    public long Sum;
    public ulong Next8Bytes;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public unsafe bool TryUpdateEntry(uint first4Bytes, uint last4Bytes, int cityNameLength, ref byte cityNameStart, int number, uint mapIndex, List<uint> mapIndexes)
    {
        // Verify that the city name matches this entries' name
        if (First4Bytes == first4Bytes && Last4Bytes == last4Bytes && Length == cityNameLength)
        {
            if (cityNameLength > 8)
            {
                byte* remainderStart = ((byte*)Unsafe.AsPointer(ref cityNameStart)) + 4;
                if (cityNameLength <= 16)
                {
                    var curNext8Bytes = *(ulong*)remainderStart;
                    curNext8Bytes &= (~0UL) >> (8 * (16 - cityNameLength));
                    if (curNext8Bytes != Next8Bytes)
                        return false;
                }
                else
                {
                    var remainderLenToCompare = Length - 8;
                    var entryRemainderSpan = new ReadOnlySpan<byte>((byte*)(nuint)Next8Bytes, remainderLenToCompare);
                    var curRemainderSpan = new ReadOnlySpan<byte>(remainderStart, remainderLenToCompare);
                    if (!entryRemainderSpan.SequenceEqual(curRemainderSpan))
                        return false;
                }
            }

            Count++;
            Sum += number;
            if (number < Min)
                Min = number;
            else if (number > Max)
                Max = number;

            return true;
        }
        
        // Otherwise, if this entry has no length it means it is uninitialized and this entry can be used for the new city
        if (Length == 0)
        {
            First4Bytes = first4Bytes;
            Last4Bytes = last4Bytes;
            Length = cityNameLength;
            Min = number;
            Max = number;
            Sum = number;

            if (cityNameLength > 8)
            {
                byte* remainderStart = ((byte*)Unsafe.AsPointer(ref cityNameStart)) + 4;
                Next8Bytes = cityNameLength <= 16
                    ? *(ulong*)remainderStart & (~0UL) >> (8 * (16 - cityNameLength))
                    : (nuint)remainderStart;
            }

            mapIndexes.Add(mapIndex);
            return true;
        }

        return false;
    }

    [SkipLocalsInit]
    public readonly unsafe string GetCityName()
    {
        byte* cityNameBuffer = stackalloc byte[100];
        *(uint*)cityNameBuffer = First4Bytes;

        if (Length > 4)
        {
            var remainderSize = Length - 4;
            if (Length <= 16)
            {
                *(ulong*)(cityNameBuffer + 4) = Next8Bytes;
                *(uint*)(cityNameBuffer + remainderSize) = Last4Bytes;
            }
            else
            {
                Buffer.MemoryCopy((byte*)(nuint)Next8Bytes, cityNameBuffer + 4, remainderSize, remainderSize);
            }
        }

        return Encoding.UTF8.GetString(cityNameBuffer, Length);
    }
}

public record CityStatistics(string CityName, int Min, int Max, int Count, long Sum)
{
    public CityStatistics AddNumber(int number) => new(CityName, Math.Min(Min, number), Math.Max(Max, number), Count + 1, Sum + number);
    public CityStatistics Merge(CityStatistics other) => new(CityName, Math.Min(Min, other.Min), Math.Max(Max, other.Max), Count + other.Count, Sum + other.Sum);
    public string ToSummary()
    {
        var avg = (Sum * 2 / Count + Math.Sign(Sum)) / 2; // fast average with rounding
        Debug.Assert(Math.Round((double)Sum / Count) == avg);
        return $"{CityName}={Min / 10}.{Math.Abs(Min % 10)}/{avg / 10}.{Math.Abs(avg % 10)}/{Max / 10}.{Math.Abs(Max % 10)}";
    }
}

public class OneBrcSolver
{
    // Chunks will be processed in parallel using PLinq, the size of each chunk must be a multiple of chunkSizeFactor
    // 65536 was chosen since on my dev machine it ensured that each chunk would align with the start of a mmap page
    private const int numChunks = 256;
    private const int chunkSizeFactor = 65536;

    public void Run(string filePath)
    {
        var fileLength = new FileInfo(filePath).Length;

        using var mmf = MemoryMappedFile.CreateFromFile(filePath);

        var chunkStatistics = Enumerable.Range(0, numChunks)
            .AsParallel()
            .Select(i => RunChunk(i, mmf, fileLength))
            .ToArray();

        Dictionary<string, CityStatistics> allStatistics = MergeChunkStatistics(chunkStatistics);

        // handle lines that overlapped consecutive chunks
        for (int i = 0; i < numChunks; i++)
        {
            var offset = i * fileLength / numChunks;
            RunAtOffset(offset - (offset % chunkSizeFactor), mmf, fileLength, allStatistics);
        }

        // handle last line at end of file that did not fit in vector width
        RunAtOffset(fileLength - (fileLength % Vector256<byte>.Count), mmf, fileLength, allStatistics, untilEnd: true);
        PrintStatistics(allStatistics);
    }

    private static unsafe CityStatistics[] RunChunk(int chunkNumber, MemoryMappedFile mmf, long fileLength)
    {
        var offset = chunkNumber * fileLength / numChunks;
        offset -= offset % chunkSizeFactor;

        var nextOffset = (chunkNumber + 1) * fileLength / numChunks;
        nextOffset = Math.Min(nextOffset, fileLength);
        nextOffset -= nextOffset % chunkSizeFactor;

        var length = nextOffset - offset;
        length -= length % Vector256<byte>.Count; // ensure length is a multiple of the vector width

        using var accessor = mmf.CreateViewAccessor(offset, length, MemoryMappedFileAccess.Read);
        byte* ptr = default;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        var chunkStatistics = GetAllMeasurementsInSpan(new ReadOnlySpan<byte>(ptr + accessor.PointerOffset, (int)length));
        accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        return chunkStatistics;
    }

    private static CityStatistics[] GetAllMeasurementsInSpan(ReadOnlySpan<byte> span)
    {
        // Chunk size factor is always a multiple of the vector count
        Debug.Assert(span.Length % Vector256<byte>.Count == 0);

        // Up to 10,000 city names are possible, give enough space to try avoid collisions, but use a power of 2 to make the modulo fast
        // Normally it's better to use a prime as the modulo, but from my testing I found that it didn't reduce collisions
        const int mapSize = 32768;

        HashEntry[] map = new HashEntry[mapSize];
        ref HashEntry mapRef = ref MemoryMarshal.GetArrayDataReference(map);

        var mapIndexes = new List<uint>(1024);

        ref byte cur = ref MemoryMarshal.GetReference(span);
        ref byte end = ref Unsafe.Add(ref cur, span.Length);

        // Find first newline and start processing there, we will handle overlapping chunks separately.
        var v = Vector256<byte>.Zero;
        uint newlines = 0;
        ref byte newlineRef = ref cur;
        uint semicolons = 0;

        Vector256<byte> allNewlines = Vector256.Create((byte)'\n');
        Vector256<byte> allSemicolons = Vector256.Create((byte)';');
        while (true)
        {
            v = Vector256.LoadUnsafe(ref cur);
            newlines = Vector256.Equals(v, allNewlines).ExtractMostSignificantBits();
            if (newlines != 0)
            {
                uint nextLineBit = newlines & (0 - newlines);
                int nextLineIndex = BitOperations.TrailingZeroCount(nextLineBit);
                newlines ^= nextLineBit;
                newlineRef = ref Unsafe.Add(ref cur, nextLineIndex);
                semicolons = Vector256.Equals(v, allSemicolons).ExtractMostSignificantBits();
                semicolons &= ~(nextLineBit - 1);
                break;
            }

            cur = ref Unsafe.Add(ref cur, Vector256<byte>.Count);
        }

        while (true)
        {
            if (semicolons == 0)
            {
                do
                {
                    cur = ref Unsafe.Add(ref cur, Vector256<byte>.Count);
                    if (Unsafe.AreSame(ref cur, ref end))
                        return BuildStatisticsArray();

                    v = Vector256.LoadUnsafe(ref cur);
                    semicolons = Vector256.Equals(v, allSemicolons).ExtractMostSignificantBits();
                } while (semicolons == 0);

                newlines = Vector256.Equals(v, allNewlines).ExtractMostSignificantBits();
            }

            uint nextSemicolonBit = semicolons & (0 - semicolons);
            int nextSemicolonIndex = BitOperations.TrailingZeroCount(nextSemicolonBit);
            semicolons ^= nextSemicolonBit;

            ref byte cityNameStart = ref Unsafe.Add(ref newlineRef, 1);
            ref byte semicolonRef = ref Unsafe.Add(ref cur, nextSemicolonIndex);

            // Ensure that the next line is contained in this chunk
            if (newlines == 0)
            {
                // no need to loop since we know that the semicolon and newline are always less than a vector width apart
                cur = ref Unsafe.Add(ref cur, Vector256<byte>.Count);
                if (Unsafe.AreSame(ref cur, ref end))
                    return BuildStatisticsArray();

                v = Vector256.LoadUnsafe(ref cur);
                newlines = Vector256.Equals(v, allNewlines).ExtractMostSignificantBits();
                semicolons = Vector256.Equals(v, allSemicolons).ExtractMostSignificantBits();
            }

            uint nextLineBit = newlines & (0 - newlines);
            int nextLineIndex = BitOperations.TrailingZeroCount(nextLineBit);
            newlines ^= nextLineBit;
            newlineRef = ref Unsafe.Add(ref cur, nextLineIndex);

            int cityNameLength = (int)Unsafe.ByteOffset(ref cityNameStart, ref semicolonRef);

            // we can be sure that there are 4 bytes available even if the length is 1 because there will be a temperature after the city name
            uint first4Bytes = Unsafe.As<byte, uint>(ref cityNameStart);
            uint last4Bytes;
            if (cityNameLength <= 4)
            {
                int numZeroBits = 8 * (4 - cityNameLength);
                first4Bytes &= (~0U) >> numZeroBits;
                last4Bytes = 0;
            }
            else
            {
                last4Bytes = Unsafe.As<byte, uint>(ref Unsafe.Subtract(ref semicolonRef, 4));
            }

            int number = ParseNumber(measurementStart: ref Unsafe.Add(ref semicolonRef, 1), measurementEnd: ref newlineRef);

            // Since the modulo for the hashmap is small, try ensure that the largest bits get factored into the hash
            // The offset are chosen in a way that you are unlikely to get bits that cancel each other out b
            var hash = first4Bytes ^ (first4Bytes >> 14) ^ (last4Bytes << 1) ^ (last4Bytes >> 13) ^ (uint)(cityNameLength << 4);
            var mapIndex = hash % mapSize;
            ref HashEntry entry = ref Unsafe.Add(ref mapRef, mapIndex);
            while (!entry.TryUpdateEntry(first4Bytes, last4Bytes, cityNameLength, ref cityNameStart, number, mapIndex, mapIndexes))
            {
                mapIndex++;
                entry = ref Unsafe.Add(ref entry, 1);
                if (mapIndex == mapSize)
                {
                    mapIndex = 0;
                    entry = ref mapRef;
                }
            }
        }

        CityStatistics[] BuildStatisticsArray()
        {
            var statistics = new CityStatistics[mapIndexes.Count];
            for (int i = 0; i < mapIndexes.Count; i++)
            {
                var entry = map[mapIndexes[i]];
                if (entry.Length > 0)
                    statistics[i] = new(entry.GetCityName(), entry.Min, entry.Max, entry.Count, entry.Sum);
            }

            return statistics;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ParseNumber(ref byte measurementStart, ref byte measurementEnd)
    {
        var isNegative = measurementStart == '-' ? 1 : 0;
        measurementStart = ref Unsafe.Add(ref measurementStart, isNegative);

        var numberLength = (int)Unsafe.ByteOffset(ref measurementStart, ref measurementEnd);

        uint numberBytes = Unsafe.As<byte, uint>(ref Unsafe.Subtract(ref measurementEnd, 4));
        numberBytes &= (~0U) << (8 * (4 - numberLength)) & 0x0F000F0FU;
        // 2561 = 256 * 10 + 1, 655361 = 256 * 256 * 10 + 1
        int number = (int)(((((numberBytes * 2561) >> 8) & 0xFF00FFU) * 655361) >> 16);
        number = (number ^ -isNegative) + isNegative;
        return number;
    }

    // Handles overlap cases such as a line in between chunks, or the the last few lines in the last chunk that don't fit in a vector
    // Since this will only get run a couple times, we don't need this to be super optimised
    private static unsafe void RunAtOffset(long offset, MemoryMappedFile mmf, long fileLength, Dictionary<string, CityStatistics> allStatistics, bool untilEnd = false)
    {
        var start = Math.Max(0, offset - 128);
        var end = untilEnd ? fileLength : offset + 128;
        var length = end - start;

        using var accessor = mmf.CreateViewAccessor(start, length, MemoryMappedFileAccess.Read);
        byte* ptr = default;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        ptr += accessor.PointerOffset;

        var span = new ReadOnlySpan<byte>(ptr, (int)length);
        var offsetInSpan = (int)(start == 0 ? offset : 128);

        // trim span so it starts at the line for the given offset
        var lineStartIndex = span[..offsetInSpan].LastIndexOf((byte)'\n') + 1;
        span = span[lineStartIndex..];

        while (!span.IsEmpty)
        {
            var measurementLength = span.IndexOf((byte)'\n');
            
            (string cityName, int number) = GetMeasurement(span[..measurementLength]);
            ref CityStatistics? dictEntry = ref CollectionsMarshal.GetValueRefOrAddDefault(allStatistics, cityName, out bool exists);
            dictEntry = exists ? dictEntry!.AddNumber(number) : new CityStatistics(cityName, number, number, 1, number);

            if (!untilEnd)
                break;

            span = span[(measurementLength + 1)..];
        }

        accessor.SafeMemoryMappedViewHandle.ReleasePointer();
    }

    // Extracts the city name and temperature measurement
    private static (string cityName, int number) GetMeasurement(ReadOnlySpan<byte> span)
    {
        var cityNameLength = span.IndexOf((byte)';');
        var cityName = Encoding.UTF8.GetString(span[..cityNameLength]);

        var tempStart = cityNameLength + 1;
        var integerPartLength = span[tempStart..].IndexOf((byte)'.');
        var integerPart = int.Parse(span.Slice(tempStart, integerPartLength));
        var decimalPart = span[tempStart + integerPartLength + 1] - '0';
        var number = integerPart * 10 + Math.Sign(integerPart) * decimalPart;
        return (cityName, number);
    }

    private static Dictionary<string, CityStatistics> MergeChunkStatistics(CityStatistics[][] chunkStatistics)
    {
        var firstChunk = chunkStatistics[0];
        var allStatistics = new Dictionary<string, CityStatistics>(firstChunk.Length * 2, StringComparer.Ordinal);

        // populate dict with values from first array
        for (int i = 0; i < firstChunk.Length; i++)
        {
            var stat = firstChunk[i];
            allStatistics[stat.CityName] = stat;
        }

        for (int i = 1; i < chunkStatistics.Length; i++)
        {
            var chunk = chunkStatistics[i];
            for (int j = 0; j < chunk.Length; j++)
            {
                var stat = chunk[j];
                ref CityStatistics? dictEntry = ref CollectionsMarshal.GetValueRefOrAddDefault(allStatistics, stat.CityName, out bool exists);
                dictEntry = exists ? dictEntry!.Merge(stat) : stat;
            }
        }

        return allStatistics;
    }

    private static void PrintStatistics(Dictionary<string, CityStatistics> allStatistics)
    {
        var sortedStatistics = allStatistics.Values.ToArray();
        Array.Sort(sortedStatistics, (a, b) => StringComparer.Ordinal.Compare(a.CityName, b.CityName));

        var sb = new StringBuilder();
        sb.Append('{');
        for (int i = 0; i < sortedStatistics.Length; i++)
        {
            if (i != 0)
                sb.Append(", ");

            sb.Append(sortedStatistics[i].ToSummary());
        }
        sb.Append('}');
        Console.WriteLine(sb.ToString());
    }
}
