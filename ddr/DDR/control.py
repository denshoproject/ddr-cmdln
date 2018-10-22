import ConfigParser
import logging
import os
import sys

from DDR import fileio

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(MODULE_PATH, 'templates')
COLLECTION_CONTROL_TEMPLATE = os.path.join(TEMPLATE_PATH, 'collection_control.tpl')
ENTITY_CONTROL_TEMPLATE     = os.path.join(TEMPLATE_PATH, 'entity_control.tpl' )


def load_template(filename):
    return fileio.read_text(filename)


class ControlFile( object ):
    """control file inspired by Debian package control file but using INI syntax.
    """
    path = None
    _config = None
    
    def __init__( self, path ):
        self.path = path
        if not os.path.exists(self.path):
            print('ERR: control file not initialized')
            sys.exit(1)
        self.read()
    
    def read( self ):
        logging.debug('    ControlFile.read({})'.format(self.path))
        self._config = ConfigParser.ConfigParser(allow_no_value=True)
        self._config.read([self.path])
    
    def write( self ):
        logging.debug('    ControlFile.write({})'.format(self.path))
        with open(self.path, 'w') as cfile:
            self._config.write(cfile)


class CollectionControlFile( ControlFile ):
    path_rel = None
    id = None
    
    def __init__(self, *args, **kwargs):
        super(CollectionControlFile, self).__init__(*args, **kwargs)
        self.id = os.path.split(os.path.dirname(self.path))[1]
        self.path_rel = os.path.basename(self.path)
    
    @staticmethod
    def create( path, collection_id ):
        logging.debug('    CollectionControlFile.create({})'.format(path))
        t = load_template(COLLECTION_CONTROL_TEMPLATE)
        fileio.write_text(t.format(cid=collection_id), path)
    
    def update_checksums( self, collection ):
        self._config.remove_section('Entities')
        self._config.add_section('Entities')
        ids = []
        [ids.append(entity.id) for entity in collection.children()]
        ids.sort()
        [self._config.set('Entities', id) for id in ids]


class EntityControlFile( ControlFile ):
    path_rel = None
    id = None
    
    def __init__(self, *args, **kwargs):
        super(EntityControlFile, self).__init__(*args, **kwargs)
        self.id = os.path.split(os.path.dirname(self.path))[1]
        sep = os.sep
        self.path_rel = sep.join(self.path.split(sep)[-3:])
    
    @staticmethod
    def create( path, collection_id, entity_id ):
        logging.debug('    EntityControlFile.create({})'.format(path))
        t = load_template(ENTITY_CONTROL_TEMPLATE)
        fileio.write_text(
            t.format(cid=collection_id, eid=entity_id),
            path
        )
    
    CHECKSUMS = ['sha1', 'sha256', 'files']
    def update_checksums( self, entity ):
        # return relative path to payload
        def relative_path(prefix_path, payload_file):
            if prefix_path[-1] != '/':
                prefix_path = '{}/'.format(prefix_path)
            return payload_file.replace(prefix_path, '')
        
        self._config.remove_section('Checksums-SHA1')
        self._config.add_section('Checksums-SHA1')
        for sha1,path in entity.checksums('sha1'):
            path = relative_path(entity.files_path, path)
            self._config.set('Checksums-SHA1', sha1, path)
        #
        self._config.remove_section('Checksums-SHA256')
        self._config.add_section('Checksums-SHA256')
        for sha256,path in entity.checksums('sha256'):
            path = relative_path(entity.files_path, path)
            self._config.set('Checksums-SHA256', sha256, path)
        #
        self._config.remove_section('Files')
        self._config.add_section('Files')
        for md5,path in entity.checksums('md5'):
            try:
                size = os.path.getsize(path)
            except:
                size = 'UNKNOWNSIZE'
            path = relative_path(entity.files_path, path)
            self._config.set('Files', md5, '{} ; {}'.format(size,path))
