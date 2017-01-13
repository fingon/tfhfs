# In-memory dict
## Write 5800 megabytes
Command: dd "if=/Users/mstenber/software/mac/10-11-elcapitan/Install OS X El Capitan.app/Contents/SharedSupport/InstallESD.dmg" of=/tmp/x/foo.dat bs=1024000

Took 78.22932720184326 seconds
74.0 megabytes per second

## Write 52122 files
Command: rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/

Took 133.9506528377533 seconds
389.0 files per second

# SQLite compressed+encrypted
## Write 5800 megabytes
Command: dd "if=/Users/mstenber/software/mac/10-11-elcapitan/Install OS X El Capitan.app/Contents/SharedSupport/InstallESD.dmg" of=/tmp/x/foo.dat bs=1024000

Took 84.90400791168213 seconds
68.0 megabytes per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 83.8937599658966 seconds
69.0 megabytes per second

## Write 52122 files
Command: rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/

Took 163.12579703330994 seconds
319.0 files per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 127.9792091846466 seconds
407.0 files per second

# SQLite encrypted
## Write 5800 megabytes
Command: dd "if=/Users/mstenber/software/mac/10-11-elcapitan/Install OS X El Capitan.app/Contents/SharedSupport/InstallESD.dmg" of=/tmp/x/foo.dat bs=1024000

Took 102.18571972846985 seconds
56.0 megabytes per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 82.43989133834839 seconds
70.0 megabytes per second

## Write 52122 files
Command: rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/

Took 176.80081796646118 seconds
294.0 files per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 136.34316205978394 seconds
382.0 files per second

# SQLite
## Write 5800 megabytes
Command: dd "if=/Users/mstenber/software/mac/10-11-elcapitan/Install OS X El Capitan.app/Contents/SharedSupport/InstallESD.dmg" of=/tmp/x/foo.dat bs=1024000

Took 94.54173111915588 seconds
61.0 megabytes per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 71.61502194404602 seconds
80.0 megabytes per second

## Write 52122 files
Command: rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/

Took 171.3370521068573 seconds
304.0 files per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 126.35336017608643 seconds
412.0 files per second

