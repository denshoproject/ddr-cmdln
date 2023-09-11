import click

from DDR import config
from DDR import fileio
from DDR import identifier

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


@click.command()
@click.option('--basedir','-b', default=config.MEDIA_BASE, help='Repository base directory')
def ddrfar(basedir):
    """Print CSV linking FAR facility with each page's DDR File ID
    
    \b
    BASEDIR  - DDR repository base directory (ddrlocal.cfg [cmdln] media_base)
    """
    headers = ['facility', 'page', 'file_id', 'file_label']
    click.echo(fileio.write_csv_str(headers))
    for facility in FAR_FACILITIES:
        eid = FACILITY_DDRCOLLECTION[facility]
        e = identifier.Identifier(eid, basedir).object()
        for n,file_ in enumerate(e.children()):
            if n == 0:
                continue
            row = [facility, n, file_.identifier.id, file_.label]
            click.echo(fileio.write_csv_str(row))


if __name__ == '__main__':
    ddrfar()
