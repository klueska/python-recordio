# Python RecordIO

Provides facilities for "Record-IO" encoding of data.
"Record-IO" encoding allows one to encode a sequence
of variable-length records by prefixing each record
with its size in bytes:

```
5\n
hello
6\n
world!
```

Note that this currently only supports record lengths
encoded as base 10 integer values with newlines as a
delimiter. This is to provide better language portability
portability: parsing a base 10 integer is simple. Most
other "Record-IO" implementations use a fixed-size header
of 4 bytes to directly encode an unsigned 32 bit length.
