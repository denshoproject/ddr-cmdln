Kura -- Storehouse
==================

A suite of command-line tools for manipulating DDR objects.


Kura understands basic METS

Kura can add a mets.xml template to an entity as part of initialization.
Kura can also manipulate certain parts of the mets.xml:
* <METS:mets>: OBJID, TYPE, LABEL
* <METS:metsHdr>: CREATEDATE, LASTMODDATE, <METS:agent>
* <METS:fileSec>

All other mets.xml manipulations should be done by some other tool.

Kura does no validation -- that is the job of the offical validator tool.
