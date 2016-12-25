# In-memory dict
## Write 5800 megabytes
Command: dd "if=/Users/mstenber/software/mac/10-11-elcapitan/Install OS X El Capitan.app/Contents/SharedSupport/InstallESD.dmg" of=/tmp/x/foo.dat bs=1024000

Took 46.79194498062134 seconds
123.0 megabytes per second

## Write 52122 files
Command: rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/

Took 130.3254199028015 seconds
399.0 files per second

# SQLite compressed+encrypted
## Write 5800 megabytes
Command: dd "if=/Users/mstenber/software/mac/10-11-elcapitan/Install OS X El Capitan.app/Contents/SharedSupport/InstallESD.dmg" of=/tmp/x/foo.dat bs=1024000

Took 135.78562188148499 seconds
42.0 megabytes per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 57.14152407646179 seconds
101.0 megabytes per second

## Write 52122 files
Command: rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/

Took 166.1662232875824 seconds
313.0 files per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 125.61733675003052 seconds
414.0 files per second

# SQLite encrypted
## Write 5800 megabytes
Command: dd "if=/Users/mstenber/software/mac/10-11-elcapitan/Install OS X El Capitan.app/Contents/SharedSupport/InstallESD.dmg" of=/tmp/x/foo.dat bs=1024000

Took 134.98714113235474 seconds
42.0 megabytes per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 53.88020586967468 seconds
107.0 megabytes per second

## Write 52122 files
Command: rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/

Took 178.03736996650696 seconds
292.0 files per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 125.01372694969177 seconds
416.0 files per second

