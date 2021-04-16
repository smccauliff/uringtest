# uringtest

This is an example of using io_uring to copy a file using O_DIRECT and registered buffers
like one might do if you were writing a database of some kind.

Creating the initial file.
dd if=/dev/zero of=/tmp/stuff bs=4096 count=1024

Tested on
> lsb_release -a
> No LSB modules are available.
> Distributor ID:	Ubuntu
> Description:	Ubuntu 20.10
> Release:	20.10
> Codename:	groovy
