Hardware: nMP (2013 Mac Pro), enough RAM, fast enough SSD

# Test #1

Test dataset: 1626MB of text files (~4500)

## write

- fresh mount /tmp/x

time rsync -av ~/bat/logs /tmp/x/

# Test #2

Test dataset: OS X El Capitan images, 6GB?

## write

time rsync -av /Users/mstenber/software/mac/10-11-elcapitan /tmp/x/

# read test

- fresh mount /tmp/x

time ( find /tmp/x -type f | xargs cat > /dev/null )

# 24.12.2016

## git commit 4ac41780c8a14b4f796832327b94d0c7b111af7e

### in-memory, no compression

(test#1) 40MB/s write (20% of time spent in SHA256)
(test#1) 42MB/s write (no profiling)
(test#2) 56MB/s write (no profiling)

## git commit 602f3a069332446ed6bfee13dbbfdcf27edf82ed

### in-memory, no compression

(test#1) ~23MB/s write

### disk SQLite, no compression

#### write

(test#1) 17MB/s

#### read

(test#1) 1st: 10.7s (= 150MB/s)
(test#1) 2nd: 1.67s (= 1G/s)


### disk SQLite, compression

#### write

(test#1) 17.5MB/s

#### read

(test#1) 1st: 11s (=~ 150MB/s)
(test#1) 2nd: 1.75s (= 1GB/s)
