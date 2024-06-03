#! /usr/bin/env python
#
# ddr-iaconvert.py
# Creates CSV for batch upload to ia using cli tool
#  

description = """Converts DDR csv metadata into IA's cli upload csv format."""

epilog = """
This command converts DDR metadata into a CSV file formatted for use with Internet 
Archive's (IA) command-line upload tool (https://github.com/jjjake/internetarchive). 
The command examines a given directory of DDR binary files and associated metadata 
CSV file that has been exported from the DDR system.
EXAMPLE
  $ ddr-iaconvert.py ./ddr-densho-1-entities.csv ./ddr-densho-1-files.csv
  $ ddr-iaconvert.py -b ./ddr-entities.csv ./ddr-files.csv ./output/ ./binaries-in/
"""

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

import argparse
import sys, shutil, os
import datetime
import csv 
import converters

def load_data(csvpath,data):
    csvfile = open(csvpath, 'r')
    csvreader = csv.DictReader(csvfile)
    for row in csvreader:
        data.append(row)
    return data

def build_dict(seq, key):
    return dict((d[key], dict(d, index=index)) for (index, d) in enumerate(seq))

#Caution! segnumber and totalsegs should be strings!
def generate_link_text(parentid,segnumber,totalsegs):
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

    if prevlink != '' and nextlink != '':
        nextlink = "  --  " + nextlink
        
    if prevlink != '' or nextlink != '':
        nextlink = nextlink + "<p>"

    return prevlink + nextlink


# Main
def parseCreators(rawcreators):
    creators = converters.text_to_rolepeople(rawcreators)
    return creators

def getMediaType(mimetype):
    mediatypemap = {
        'video': 'movies',
        'audio': 'audio',
        'image': 'image', 
        'application': 'texts', #get pdfs
        'text': 'texts'
    }
    return mediatypemap.get(mimetype.split('/')[0])

def getDescription(isSegment,identifier,descrip,location,segnum,totalsegs):
    description = ''
    sequenceinfo = ''
    if isSegment:
        sequenceinfo = 'Segment {} of {}<p>{}'.format(segnum,totalsegs,generate_link_text(identifier[:identifier.rfind('-')],segnum,totalsegs))
    locationinfo = 'Interview location: {}'.format(location) if isSegment else 'Location: {}'.format(location)
    denshoboilerplate = 'See this item in the <a href=\"https://ddr.densho.org/\" target=\"blank\" rel=\"nofollow\">Densho Digital Repository</a> at: <a href=\"https://ddr.densho.org/{}/\" target=\"_blank\" rel=\"nofollow\">https://ddr.densho.org/{}/</a>.'.format(identifier,identifier)
    description = locationinfo + '<p>' + descrip + '<p>' + sequenceinfo + denshoboilerplate
 
    return description

def getCreators(creatorsdata):
    creators = ''
    for i, c in enumerate(creatorsdata):
        creators += '{}: {}{}'.format(c['role'].capitalize(),c['namepart'],'' if i == (len(creatorsdata) - 1) else ', ')
    return creators

def getCredits(personnel):
    credits = ''
    for i, c in enumerate(personnel):
        credits += '{}: {}{}'.format(c['role'].capitalize(),c['namepart'],'' if i == (len(personnel) - 1) else ', ')
    return credits

def getLicense(code):
    if code == 'cc':
        licenseurl = 'https://creativecommons.org/licenses/by-nc-sa/4.0/'
    elif code == 'pdm':
        licenseurl = 'http://creativecommons.org/publicdomain/mark/1.0/'
    else:
        licenseurl =''

    return licenseurl

def getFirstFacility(rawfacilities):
    facility = ''
    facilitydata = converters.text_to_listofdicts(rawfacilities)
    if facilitydata:
        facility = facilitydata[0]['term']
    return facility

def isExternal(external):
    if (external == '1') or (external.lower() == 'true'):
        return True
    else:
        return False

def doConvert(entcsv,filecsv,outputpath,prep_binaries,binariespath):
    entdata = []
    load_data(entcsv,entdata)
    filedata = []
    load_data(filecsv,filedata)
    
    print('entdata length: {}'.format(str(len(entdata))))
    print('filedata length: {}'.format(str(len(filedata))))
    
    entities_by_ddrid = build_dict(entdata,key="id")
    
    #set up output csv; write headers
    outputfile = os.path.join(os.path.abspath(outputpath),'{:%Y%m%d-%H%M%S}-iaconvert.csv'.format(datetime.datetime.now()))
    odatafile = open(outputfile,'w')
    outputwriter = csv.writer(odatafile)
    outputwriter.writerow(['identifier',
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
                            'runtime'])
    odatafile.close()

    #iterate through files
    for f in filedata:
        #TODO make logic understand multiple boolean forms
         if isExternal(f['external']):
            if 'mezzanine' in f['id'] or 'master' in f['id'] or 'transcript' in f['id']:
                ddrid = f['id'][:f['id'].rindex('-',0,f['id'].rindex('-'))]
            else:
                ddrid = f['id']
            print('file {}. processing...'.format(ddrid))

            if ddrid in entities_by_ddrid:
                ent = entities_by_ddrid[ddrid]
                identifier = ent['id']
                interviewid = ''
                creators_parsed = parseCreators(ent['creators'])
                totalsegs = 0
                isSegment = True if ent['format'] == 'vh' else False
                if isSegment:
                    #get the interview id
                    interviewid = entities_by_ddrid[ddrid[:ddrid.rfind('-')]]['id']
                    print('interviewid: {}'.format(interviewid))
                    #get segment info
                    for s in entities_by_ddrid:
                        check = s[0:100]
                        if check.startswith(interviewid):
                           totalsegs +=1
                           print('found a segment for {}. check={}. totalsegs={}'.format(interviewid,check,totalsegs))
                    #must account for interview entity in entities_by_ddrid
                    totalsegs -=1

                filename = '{}-{}-{}{}'.format(identifier,f['role'],f['sha1'][:10],f['basename_orig'][f['basename_orig'].rfind('.'):])

                if prep_binaries:
                    origfile = os.path.join(binariespath,f['basename_orig'])
                    if os.path.exists(origfile):
                        shutil.copy2(origfile,os.path.join(outputpath,filename))
                    else:
                        print('{:%Y%m%d %H:%M:%S.%s}: Error - {} does not exist. Could not prep binary for {}.'.format(datetime.datetime.now(),origfile,identifier))
 
                #note this is the IA collection bucket; not the DDR collection
                collection = interviewid if isSegment else 'densho'
                mediatype = getMediaType(f['mimetype'])
                description = getDescription(isSegment,identifier,ent['description'],ent['location'],ent['sort'],str(totalsegs))
                title = ent['title']
                contributor = ent['contributor']
                creator = getCreators(creators_parsed)
                date = ent['creation']
                subject0 = 'Japanese Americans'
                subject1 = 'Oral history'
                #if entity has facility, get the first one
                subject2 = getFirstFacility(ent['facility'])
                licenseurl = getLicense(ent['rights'])
                credits = getCredits(creators_parsed)
                runtime = ent['extent']
                
                #write the row to csv
                odatafile = open(outputfile,'a')
                outputwriter = csv.writer(odatafile)
                outputwriter.writerow([identifier,
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
                                        runtime])
                odatafile.close()

    return

def main():

    parser = argparse.ArgumentParser(description=description, epilog=epilog,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('entitycsv', help='Path to DDR entities csv file.')
    parser.add_argument('filecsv', help='Path to DDR files csv file.')
    parser.add_argument('outputpath', nargs='?', default=os.getcwd(), help='Path to save output.')
    parser.add_argument('binariespath', nargs='?', default=os.getcwd(), help='Path to original binaries for prep.')
    parser.add_argument('-b', '--prep-binaries', action='store_true', dest='prep_binaries', help='Prep binaries for upload. Uses binariespath argument.')

    args = parser.parse_args()
    print('Entity csv path: {}'.format(args.entitycsv))
    print('File csv path: {}'.format(args.filecsv))
    print('Output path: {}'.format(args.outputpath))
    print('Binaries path: {}'.format(args.binariespath))
    if args.prep_binaries:
        print('Prep binaries mode activated.')

    started = datetime.datetime.now()
    inputerrs = ''
    if not os.path.isfile(args.entitycsv):
        inputerrs + 'Entities csv does not exist: {}\n'.format(args.entitycsv)
    if not os.path.isfile(args.filecsv):
        inputerrs + 'Files csv does not exist: {}'.format(args.filecsv)
    if not os.path.exists(args.outputpath):
        inputerrs + 'Output path does not exist: {}'.format(args.outputpath)
    if not os.path.exists(args.binariespath):
        inputerrs + 'Binaries path does not exist: {}'.format(args.binariespath)       
    if inputerrs != '':
        print('Error -- script exiting...\n{}'.format(inputerrs))
    else:
        doConvert(args.entitycsv,args.filecsv,args.outputpath,args.prep_binaries,args.binariespath)
    
    finished = datetime.datetime.now()
    elapsed = finished - started
    
    print('Started: {}'.format(started))
    print('Finished: {}'.format(finished))
    print('Elapsed: {}'.format(elapsed))
    
    return

if __name__ == '__main__':
    main()
