# The One Billion Row Challenge
This is my C# implementation for the One Billion Row Challenge (1BRC) as defined in the [gunnarmorling/1brc](https://github.com/gunnarmorling/1brc) GitHub repository.

## Running my solution
Make sure you have the latest .NET 8 SDK installed. Building the project is done using the following commands (change the runtime identifier from win-x64 to the [one for your OS](https://learn.microsoft.com/en-us/dotnet/core/rid-catalog)):

```cmd
dotnet build -c Release
dotnet publish -r win-x64 -c Release
```

Then you can find the `1brc.exe` inside the `bin/Release/net8.0/publish/win-x64` directory. This executable takes a single argument which is the file path to the measurements file.

## Measurements
My system has the following specs
- CPU: AMD Ryzen 9 5950X @ 3.4GHz (default clock speed)
- RAM: 32GB 3600MHz DDR4
- SSD: Samsung 980 PRO
- OS: Windows 11

I generated my input file using the `CreateMeasurements` script from the original repo. They recently changed it so that it doesn't generate carriage returns on Windows, so my solution assumes that lines are separated only by newlines. 

I have a `Stopwatch` that is started when the program starts and prints the elapsed time just before the program exits. I also am using [pbench](https://github.com/chemodax/pbench) to time how long it takes to invoke the whole program so that it includes the time spent launching the runtime. I have my application compiled with NativeAOT so there is no JIT needed. 

After running my program 10 times in a row, these are my measurements on my system:
- Stopwatch: Min=1.318s, Avg=1.335s, Max=1.350s
- Process Time: Min=1.329s, Avg=1.346s, Max=1.361s

## Comparison to other solutions
I also ran the C# solution written by [buybackoff](https://github.com/buybackoff/1brc) which also has a Stopwatch at the start and stop of the program. I noticed when running it that there was a much larger gap between the Stopwatch time and the process time. I believe this is because the stopwatch time is not timing how long it takes to close/dispose any of the file handles. I have a suspicion that maybe these issues are only showing up because I am using Windows and the results would be different on Linux.

And I also ran royvanrijn's Java solution which is currently winning the competition in the original repo. I ran it using the latest GraalVM JDK. It does not have a stopwatch in the source code, so I can only time the whole process time.

buybackoff's C# solution:
- Stopwatch: Min=1.433s, Avg=1.453s, Max=1.503s
- Process Time: Min=2.202s, Avg=2.215s, Max=2.270s

royvanrijn's Java solution:
- Process Time: Min=2.501s, Avg=2.549s, Max=2.597s