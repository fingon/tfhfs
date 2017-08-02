# SQLite compressed+encrypted
## Write 4325 megabytes
Command: dd "if=/Volumes/ulko/share/2/software/unix/2015-09-24-raspbian-jessie.img" of=/tmp/x/foo.dat bs=1024000

Took 51.47963213920593 seconds
84.0 megabytes per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 36.58484983444214 seconds
118.0 megabytes per second

## Write 56711 files
Command: rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/

Took 264.1169590950012 seconds
214.0 files per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 204.4691550731659 seconds
277.0 files per second

# SQLite encrypted
## Write 4325 megabytes
Command: dd "if=/Volumes/ulko/share/2/software/unix/2015-09-24-raspbian-jessie.img" of=/tmp/x/foo.dat bs=1024000

Took 53.56386089324951 seconds
80.0 megabytes per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 44.01934003829956 seconds
98.0 megabytes per second

## Write 56711 files
Command: rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/

Took 283.02888083457947 seconds
200.0 files per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 213.89857411384583 seconds
265.0 files per second

# SQLite
## Write 4325 megabytes
Command: dd "if=/Volumes/ulko/share/2/software/unix/2015-09-24-raspbian-jessie.img" of=/tmp/x/foo.dat bs=1024000

Took 59.66087007522583 seconds
72.0 megabytes per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 25.23131799697876 seconds
171.0 megabytes per second

## Write 56711 files
Command: rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/

Took 275.87994718551636 seconds
205.0 files per second

## Read it back
Command: find /tmp/x -type f | xargs cat > /dev/null

Took 202.33411574363708 seconds
280.0 files per second

