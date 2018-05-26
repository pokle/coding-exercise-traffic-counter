# Traffic counter

- Line by line streaming
    - Tested on a large file, and never takes more than 12mb ram while running
- Not so happy about the way I had to print text while grouping - but it's pragmatic I suppose.

## Running it
Ensure you have python 3 installed (brew install python3)

Verify everything's in order by running the tests: `make test`

Then you're good to go:

```
$ python3 traffic_report.py data.file
## Count of cars grouped by date
2016-12-01 179
2016-12-05 81
2016-12-08 134
2016-12-09 4
## Total cars =  398
## Top 3 half hours
2016-12-01T07:30:00 46
2016-12-01T08:00:00 42
2016-12-08T18:00:00 33
## 1.5 hour period with ​least​ cars = 20 cars [2016-12-01T15:00:00 .. 2016-12-01T23:30:00]
```