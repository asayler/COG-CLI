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
$ ./cog-cli.py --url <API URL> util save-config <NAME>
```

Where `<API URL>` is something like `https://api-cog-csci1300.cs.colorado.edu`
and `<NAME>` is something like `csci1300`.

You will be prompted for your username and password. Use your COG
(i.e. Moodle/Identikey) credentials.


### Creating an Assignment ###

To create a new assignment, prep the necessary grader files into a zip
file and run:

```
$ ./cog-cli.py --server <SERVER NAME> util setup-assignment \
  --asn_name "<ASSIGNMENT NAME>" --env "local" \
  --tst_name "<TEST NAME>" --tester "script" --maxscore <SCORE> \
  --path <ZIP PATH>.zip --extract --path_script '<SCRIPT PATH>' \
  --rptmod moodle --rptmod_opt moodle_cm_id <CMID>
```

Where `<SERVER NAME>` is the value you set during the save-config
command above, `<ASSIGNMENT NAME>` is your chosen assignment name,
`<TEST NAME>` is your chosen test name, `<SCORE>` is the max score,
`<ZIP PATH>` is the location of the grading script file or archive to
upload, `--extract` ensures the uploaded archive is extracted (ignore
this option for single-file uploads), `<SCRIPT PATH>` is the relative
path to the grading script inside the grading archive, and `<CMID>` is
the web ID of the Moodle assignment you want to report grades to
(e.g. `moodle.cs.colorado.edu/mod/assign/view.php?id=<CMID>`).

You may wish to save the output of this command since the UUIDs it
produces will be necessary in subsequent commands.

You can add the option `--activate` option to activate the assignment
immediately, thus avoiding the next step.

There are also a number of extra Moodle reporter parameters that can
be used to enable additional Moodle reporter functionality:

+ `moodle_late_penalty`: points to deduct per `moodle_late_period`
  past due date
+ `moodle_late_period`: late period in seconds (e.g. 86400 = 1 day)
+ `moodle_prereq_cm_id`: Moodle cmid of prereq assignment
+ `moodle_prereq_min`: Minimum grade stunt must receive on prereq
  assignment before being graded on this assignment.

The `moodle_late_*` parameters can be used to setup a linear late
penalty that is applied for any COG submission that occurred after the
Moodle assignment due date but before the Moodle assignment cutoff
date.

The `moodle_prereq_*` parameters can be used to specify a prerequisite
assignment on which the student must receive a threshold grade before
being graded on the current assingmnt.

To use these parameters, specify them as key:value pairs following an
`--rptmod_opt` option as shown in the example above.

### Activating/Deactivating an Assignment ###

An existing assignment can be activated/deactivate to control whether or
not it shows up on the COG Web GUI:

```
$ ./cog-cli.py --server <SERVER NAME> assignment activate --uid <ASSIGNMENT UUID>
$ ./cog-cli.py --server <SERVER NAME> assignment deactivate --uid <ASSIGNMENT UUID>
```

Where `<SERVER NAME>` is the value you set during the `save-config` command
above and `<Assignment UUID>` is the UUID of the assignment in
question.


### Updating Test Files ###

If you need to upload/replace the grader script files for a given
test:

```
$ ./cog-cli.py --server <SERVER NAME> util replace-test-files \
               --tst_uid <TEST UUID> \
               --path <PATH>.zip --extract
```

Where `<SERVER NAME>` is the value you set during the save-config command
above, `<PATH>` is the location of the grading script file or archive
to upload, `--extract` ensures the uploaded archive is extracted
(ignore this option for single-file uploads), and `<TEST UUID>` is the
UUID of the test on which you wish to replace the files.


Related
-------

 * [COG](https://github.com/asayler/COG): COG Backend and API
 (this is where the magic happens)
 * [COG-Web](https://github.com/asayler/COG-Web): Web Front-end


Licensing
---------

Copyright 2014, 2015, 2016 by Andy Sayler

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
