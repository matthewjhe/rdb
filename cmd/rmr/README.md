# RMR

RMR is a command line util to generate Redis Memory Report.

## Usage

```
Usage: ./rmr [options] -f /path/to/dump.rdb

Options:

  -b int
        Read buffer size.
  -d	Enable debug output.
  -db value
        Databases to inspect. Multiple databases can provided.
  -f string
        Redis RDB file path.
  -k value
        Keys to inspect. Multiple keys can provided.
  -m int
        Maximum memory mapping size. (default 1073741824)
  -o string
        Output file.
  -p value
        Key match patterns. Multiple patterns can provided.
  -t value
        Types to inspect. Multiple types can provided.
```
