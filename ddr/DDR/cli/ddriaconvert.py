import csv 
import datetime
import os
import shutil
import sys

import click

from DDR import converters
from DDR import identifier

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--binaries', '-b', default='', help='Path to original binaries for prep.')
@click.argument('entitycsv')
@click.argument('filecsv')
@click.argument('outputpath')
def ddriaconvert(binaries, entitycsv, filecsv, outputpath):
    """Converts DDR csv metadata into IA's cli upload csv format.
    
    \b
    This command converts DDR metadata into a CSV file formatted for use with Internet
    Archive's (IA) command-line upload tool (https://github.com/jjjake/internetarchive).
    The command examines a given directory of DDR binary files and associated metadata
    CSV file that has been exported from the DDR system.
    
    \b
    EXAMPLE
    $ ddriaconvert ./ddr-densho-1-entities.csv ./ddr-densho-1-files.csv ./output/
    $ ddriaconvert ./ddr-entities.csv ./ddr-files.csv ./output/ --binaries ./binaries-in/
    """
    print('Entity csv path: {}'.format(entitycsv))
    print('File csv path: {}'.format(filecsv))
    print('Output path: {}'.format(outputpath))
    binariespath = binaries
    if binariespath:
        print('Binaries path: {}'.format(binariespath))
        print('Prep binaries mode activated.')

    inputerrs = 0
    if not os.path.isfile(entitycsv):
        click.echo(f"ERROR: Entities csv does not exist: {entitycsv}")
        inputerrs += 1
    if not os.path.isfile(filecsv):
        click.echo(f"Files csv does not exist: {filecsv}")
        inputerrs += 1
    if not os.path.exists(outputpath):
        click.echo(f"Output path does not exist: {outputpath}")
        inputerrs += 1
    if binariespath and not os.path.exists(binariespath):
        click.echo(f"Binaries path does not exist: {binariespath}")
    if inputerrs:
        sys.exit(1)

    started = datetime.datetime.now()
    
    outputfile = do_conversion(entitycsv, filecsv, outputpath, binariespath)
    
    finished = datetime.datetime.now()
    elapsed = finished - started
    print('Started: {}'.format(started))
    print('Finished: {}'.format(finished))
    print('Elapsed: {}'.format(elapsed))

    return outputfile


def load_data(csvpath):
    with open(csvpath, 'r') as f:
        return [row for row in csv.DictReader(f)]

def build_dict(entity_data):
    return {
        entity['id']: entity
        for entity in entity_data
    }

#Caution! segnumber and totalsegs should be strings!
def generate_link_text(parentid, segnumber, totalsegs):
    prefix = parentid + '-'
    if totalsegs == '1':
        nextid = None
        previd = None
    elif segnumber == '1':
        previd = None
        nextid = prefix + str(int(segnumber) + 1)
    elif int(segnumber) < int(totalsegs):
        nextid = prefix + str(int(segnumber) + 1)
        previd = prefix + str(int(segnumber) - 1)
    else:
        nextid = None
        previd = prefix + str(int(segnumber) - 1)
        
    if nextid:
        nextlink = '[ <a href=\"https://archive.org/details/' + nextid + '\">Next segment</a> ]'
    else:
        nextlink = ''

    if previd:
        prevlink = '[ <a href=\"https://archive.org/details/' + previd + '\">Previous segment</a> ]'
    else:
        prevlink = ''

    if prevlink and nextlink:
        nextlink = "  --  " + nextlink
        
    if prevlink or nextlink:
        nextlink = nextlink + "<p>"

    return prevlink + nextlink

def parse_creators(rawcreators):
    return converters.text_to_rolepeople(rawcreators, default={})

def get_media_type(mimetype):
    mediatypemap = {
        'video': 'movies',
        'audio': 'audio',
        'image': 'image', 
        'application': 'texts',  # get pdfs
        'text': 'texts'
    }
    return mediatypemap.get(mimetype.split('/')[0])

def get_description(
        is_segment, identifier, description, location, segnum, totalsegs
):
    if is_segment:
        link_text = generate_link_text(
            identifier[:identifier.rfind('-')], segnum, totalsegs
        )
        sequenceinfo = f"Segment {segnum} of {totalsegs}<p>{link_text}"
    else:
        sequenceinfo = ''
    if is_segment:
        locationinfo = f"Interview location: {location}"
    else:
        locationinfo = f"Location: {location}"
    denshoboilerplate = 'See this item in the ' \
        '<a href="https://ddr.densho.org/" target="blank" rel="nofollow">' \
        'Densho Digital Repository' \
        '</a> at: ' \
        f'<a href="https://ddr.densho.org/{identifier}/" target="_blank" rel="nofollow">' \
        f'https://ddr.densho.org/{identifier}/' \
        '</a>.'
    return locationinfo + '<p>' + description + '<p>' + sequenceinfo + denshoboilerplate

def get_creators(creatorsdata):
    creators = [
        c['role'].capitalize() + ': ' + c['namepart']
        for c in creatorsdata
    ]
    return ', '.join(creators)

def get_credits(personnel):
    credits = [
        c['role'].capitalize() + ': ' + c['namepart']
        for c in personnel
    ]
    return ', '.join(credits)

def get_license(code):
    if code == 'cc':
        return 'https://creativecommons.org/licenses/by-nc-sa/4.0/'
    elif code == 'pdm':
        return 'http://creativecommons.org/publicdomain/mark/1.0/'
    return ''

def get_first_facility(rawfacilities):
    facilitydata = converters.text_to_listofdicts(rawfacilities)
    if facilitydata:
        return facilitydata[0]['term']
    else:
        return ''

def is_external(external):
    if (external == '1') or (external.lower() == 'true'):
        return True
    else:
        return False

"""
entities_cols:
id,status,public,title,description,creation,location,creators,language,genre,
format,extent,contributor,alternate_id,digitize_person, digitize_organization,
digitize_date,credit,topics,persons,facility,chronology,geography,parent,
rights,rights_statement,notes,sort,signature_id

files_cols:
id,external,role,basename_orig,mimetype,public,rights,sort,thumb,label,
digitize_person,tech_notes,external_urls,links,sha1,sha256,md5,size
"""

def do_conversion(entity_csv, file_csv, outputdir, binariespath):
    entity_data = load_data(entity_csv)
    file_data = load_data(file_csv)
    
    print('entity_data length: {}'.format(str(len(entity_data))))
    print('file_data length: {}'.format(str(len(file_data))))

    entities_by_ddrid = build_dict(entity_data)

    # set up output csv; write headers
    outputfile = os.path.join(
        os.path.abspath(outputdir),
        '{:%Y%m%d-%H%M%S}-iaconvert.csv'.format(datetime.datetime.now())
    )
    with open(outputfile,'w') as csvfile:
        outputwriter = csv.writer(csvfile)
        outputwriter.writerow([
            'identifier',
            'file',
            'collection',
            'mediatype',
            'description',
            'title',
            'contributor',
            'creator',
            'date',
            'subject[0]',
            'subject[1]',
            'subject[2]',
            'licenseurl',
            'credits',
            'runtime',
        ])

    # iterate through files
    for f in file_data:
        # TODO make logic understand multiple boolean forms
        if is_external(f['external']):
            #if 'mezzanine' in f['id'] or 'master' in f['id'] or 'transcript' in f['id']:
            #    ddrid = f['id'][:f['id'].rindex('-',0,f['id'].rindex('-'))]
            fi = identifier.Identifier(f['id'])
            if fi.idparts['role'] in ['master', 'mezzanine', 'transcript']:
                ddrid = fi.parent().id
            else:
                ddrid = f['id']
            print('file {}. processing...'.format(ddrid))

            if ddrid in entities_by_ddrid:
                entity = entities_by_ddrid[ddrid]
                entity_id = entity['id']
                interviewid = ''
                creators_parsed = parse_creators(entity['creators'])
                totalsegs = 0
                if entity['format'] == 'vh':
                    isSegment = True
                else:
                    isSegment = False
                if isSegment:
                    # get the interview id
                    #interviewid = entities_by_ddrid[ddrid[:ddrid.rfind('-')]]['id']
                    interviewid = identifier.Identifier(entity_id).parent().id
                    print('interviewid: {}'.format(interviewid))
                    # get segment info
                    for segment in entities_by_ddrid:
                        check = segment[0:100]
                        if check.startswith(interviewid):
                           totalsegs +=1
                           print(
                               f"found a segment for {interviewid}. " \
                               f"check={check}. totalsegs={totalsegs}"
                           )
                    # must account for interview entity in entities_by_ddrid
                    totalsegs -=1

                filename = f['id'] + os.path.splitext(f['basename_orig'])[1]

                if binariespath:
                    origfile = os.path.join(binariespath, f['basename_orig'])
                    if os.path.exists(origfile):
                        destfile = os.path.join(outputdir, filename)
                        shutil.copy2(origfile, destfile)
                    else:
                        print(f"Error: {origfile} missing.")
                        print(f"Could not prep binary for {entity_id}.")
 
                # note this is the IA collection bucket; not the DDR collection
                if isSegment:
                    collection = interviewid
                else:
                    collection = 'densho'
                mediatype = get_media_type(f['mimetype'])
                description = get_description(
                    isSegment, entity_id,
                    entity['description'], entity['location'], entity['sort'],
                    str(totalsegs)
                )
                title = entity['title']
                contributor = entity['contributor']
                creator = get_creators(creators_parsed)
                date = entity['creation']
                subject0 = 'Japanese Americans'
                subject1 = 'Oral history'
                # if entity has facility, get the first one
                subject2 = get_first_facility(entity['facility'])
                licenseurl = get_license(entity['rights'])
                credits = get_credits(creators_parsed)
                runtime = entity['extent']
                
                # write the row to csv
                with open(outputfile,'a') as csvout:
                    outputwriter = csv.writer(csvout)
                    outputwriter.writerow([
                        entity_id,
                        filename,
                        collection,
                        mediatype,
                        description,
                        title,
                        contributor,
                        creator,
                        date,
                        subject0,
                        subject1,
                        subject2,
                        licenseurl,
                        credits,
                        runtime,
                    ])

    return outputfile


if __name__ == '__main__':
    ddriaconvert()
