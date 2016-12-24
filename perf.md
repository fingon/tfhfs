Hardware: nMP (2013 Mac Pro), enough RAM, fast enough SSD

Test dataset: 1626MB of text files (~4500)


# write test

- fresh mount /tmp/x

time rsync -av ~/bat/logs /tmp/x/

# read test

- fresh mount /tmp/x

time ( find /tmp/x -type f | xargs cat > /dev/null )

# 24.12.2016

## git commit 4ac41780c8a14b4f796832327b94d0c7b111af7e

### in-memory, no compression

40MB/s write (20% of time spent in SHA256)

## git commit 602f3a069332446ed6bfee13dbbfdcf27edf82ed

### in-memory, no compression

~23MB/s write

### disk SQLite, no compression

#### write

17MB/s

#### read

1st: 10.7s (= 150MB/s)
2nd: 1.67s (= 1G/s)


### disk SQLite, compression

#### write

17.5MB/s

#### read

1st: 11s (=~ 150MB/s)
2nd: 1.75s (= 1GB/s)


