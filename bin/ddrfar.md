# Make CSV linking FAR facility codes to FAR document scan files

This script produces a CSV file linking FAR codes used in `namesdb-editor` and elsewhere with individual `File` IDs.

The Final Accountability Rosters Collection (ddr-densho-305-1) contains scans of
the original FAR documents.
Each `Entity` represents a camp and a set of documents.
Each `File` represents a single scanned page.

``` python
FAR_FACILITIES = [
    '1-topaz',
    '2-poston',
    '3-gilariver',
    '4-amache',
    '5-heartmountain',
    '6-jerome',
    '7-manzanar',
    '8-minidoka',
    '9-rohwer',
    '10-tulelake',
]

FACILITY_DDRCOLLECTION = {
    '1-topaz':         'ddr-densho-305-1',
    '2-poston':        'ddr-densho-305-2',
    '3-gilariver':     'ddr-densho-305-3',
    '4-amache':        'ddr-densho-305-4',
    '5-heartmountain': 'ddr-densho-305-5',
    '6-jerome':        'ddr-densho-305-6',
    '7-manzanar':      'ddr-densho-305-7',
    '8-minidoka':      'ddr-densho-305-8',
    '9-rohwer':        'ddr-densho-305-9',
    '10-tulelake':     'ddr-densho-305-10',
}

BASEDIR = '/var/www/media/ddr'
OUTPUT_FILE = '/tmp/ddr-far.csv'

from pathlib import Path
from DDR import config
from DDR import fileio
from DDR import identifier
headers = ['facility', 'page', 'file_id', 'file_label']
lines = [fileio.write_csv_str(headers)]
for facility in FAR_FACILITIES:
    eid = FACILITY_DDRCOLLECTION[facility]
    entity = identifier.Identifier(eid, BASEDIR).object()
    for n,file_ in enumerate(entity.children()):
        if n == 0:
            continue
        row = [facility, n, file_.identifier.id, file_.label]
        line = fileio.write_csv_str(row)
        lines.append(line)

with Path(OUTPUT_FILE).open('w') as f:
    f.write('\n'.join(lines))
```
