COG-CLI: COG Command Line Interface
===================================

By [Andy Sayler](https://www.andysayler.com)
University of Colorado, Boulder


Status
------

COG-CLI is currently running in Beta in production. Bug reports,
patches, and comments welcome.


Prereq
------

```
$ sudo apt-get install python3
$ sudo make reqs
```

Usage
-----

### Help ###

The CLI is self documenting:

```
$ ./cog-cli.py --help
$ ./cog-cli.py <group> --help
$ ./cog-cli.py <group> <command> --help
```

### Setup ###

First, setup a new config:

```
$ ./cog-cli.py util --url <API URL> save-config <NAME>
```

Where `<API URL>` is something like `api-cog-csci1300.cs.colorado.edu`
and <NAME> is something like `csci1300`.

You will be prompted for your username and password. Use your COG
(e.g. Moodle/Identikey credentials).


### Activating/Deactivating an Assignment ###

An existing assignment can be activated/deactivate to control whether or
not it shows up on the COG web GUI:

```
$ ./cog-cli.py --server <NAME> assignment activate --uid <ASSIGNMENT UUID>
$ ./cog-cli.py --server <NAME> assignment deactivate --uid <ASSIGNMENT UUID>
```

Where `<NAME>` is the value you set during the save-config command
above and `<Assignment UUID>` is the UUID of the assignment in
question.


### Updating Test Files ###

If you need to upload/replace the grader script files for a given
test:

```
$ ./cog-cli.py --server <NAME> util replace-test-files \
               --path <PATH>.zip --extract \
               --tst_uid <TEST UUID>
```

Where `<NAME>` is the value you set during the save-config command
above, `<PATH>` is the location of the grading script file or archive
to upload, `--extract` ensures the uploaded archive Si extracted
(ignore this option for single-file uploads), and `<TEST UUID>` is the
UUID of the test on which you which to replace the files.


Related
-------

 * [COG](https://github.com/asayler/COG): COG Backend and API
 (this is where the magic happens)
 * [COG-Web](https://github.com/asayler/COG-Web): Web Front-end


Licensing
---------

Copyright 2014, 2015 by Andy Sayler

This file is part of COG-CLI.

COG-CLI is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

COG-CLI is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details.

You should have received a copy of the GNU General Public License
along with COG-CLI (see COPYING).  If not, see
http://www.gnu.org/licenses/.
