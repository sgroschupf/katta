Katta: Lucene in the cloud. Or MapFiles. Or your own custom server.

For the latest information about Katta, please visit our website at:

   http://katta.sourceforge.net/

To build please:
Check out project from git:

$ git clone git://katta.git.sourceforge.net/gitroot/katta/katta (read-only)

Prepare submodules by running:
$ git submodule init
$ git submodule update

If "git submodule update" doesn't work because you are working behind a firewall that rejects the git protocol, you can
edit the .git/config file and switch the zkclient checkout URL to http://github.com/joa23/zkclient.git. After you have
done this, just run "git submodule update" again.