import simplejson as json

from DDR import dvcs


class Module(object):
    path = None

    def __init__(self, module):
        """
        @param module: collection, entity, files model definitions module
        """
        self.module = module
        self.path = None
        if self.module and self.module.__file__:
            self.path = self.module.__file__.replace('.pyc', '.py')

    def __repr__(self):
        return "<%s.%s '%s'>" % (self.__module__, self.__class__.__name__, self.path)
    
    def field_names(self):
        """Returns list of module fieldnames.
        
        @returns: list of field names
        """
        field_names = [
            field['name']
            for field in getattr(self.module, 'FIELDS', [])
        ]
        return field_names
    
    def required_fields(self, exceptions=[]):
        """Reads module.FIELDS and returns names of required fields.
        
        TODO refactor to add context (e.g. csv, form, elasticsearch, etc)
        
        >>> fields = [
        ...     {'name':'id', 'form':{'required':True}},
        ...     {'name':'title', 'form':{'required':True}},
        ...     {'name':'description', 'form':{'required':False}},
        ...     {'name':'formless'},
        ...     {'name':'files', 'form':{'required':True}},
        ... ]
        >>> exceptions = ['files', 'whatever']
        >>> batch.get_required_fields(fields, exceptions)
        ['id', 'title']
        
        @param exceptions: list of field names
        @returns: list of field names
        """
        return [
            field['name']
            for field in self.module.FIELDS
            if field.get('form', None) \
            and field['form']['required'] \
            and (field['name'] not in exceptions)
        ]

    def csv_export_fields(self, required_only=False):
        """Returns list of module fields marked for CSV export
        
        @param required_only: boolean
        @returns: list
        """
        # In repo_models.object.FIELDS, individual fields can be marked
        # so they are ignored (e.g. not included) when exporting.
        export_directives = {
            f['name']: f['csv']['export']
            for f in self.module.FIELDS
        }
        return [
            f for f in self.field_names()
            if not ('ignore' in export_directives[f])
        ]
    
    def is_valid(self):
        """Indicates whether this is a proper module
    
        TODO determine required fields for models
    
        @returns: Boolean,str message
        """
        if not self.module:
            return False,"%s has no module object." % self
        # Is the module located in a 'ddr' Repository repo?
        # collection.__file__ == absolute path to the module
        match = 'ddr/repo_models'
        if not match in self.module.__file__:
            return False,"%s not in 'ddr' Repository repo." % self.module.__name__
        # is fields var present in module?
        fields = getattr(self.module, 'FIELDS', None)
        if not fields:
            return False,'%s has no FIELDS variable.' % self.module.__name__
        # is fields var listy?
        if not isinstance(fields, list):
            return False,'%s.FIELDS is not a list.' % self.module.__name__
        return True,'ok'
    
    def function(self, function_name, value):
        """If named function is present in module and callable, pass value to it and return result.
        
        Among other things this may be used to prep data for display, prepare it
        for editing in a form, or convert cleaned form data into Python data for
        storage in objects.
        
        @param function_name: Name of the function to be executed.
        @param value: A single value to be passed to the function, or None.
        @returns: Whatever the specified function returns.
        """
        if (function_name in dir(self.module)):
            function = getattr(self.module, function_name)
            value = function(value)
        return value
    
    def xml_function(self, function_name, tree, NAMESPACES, f, value):
        """If module function is present and callable, pass value to it and return result.
        
        Same as Module.function but with XML we need to pass namespaces lists to
        the functions.
        Used in dump_ead(), dump_mets().
        
        @param function_name: Name of the function to be executed.
        @param tree: An lxml tree object.
        @param NAMESPACES: Dict of namespaces used in the XML document.
        @param f: Field dict (from MODEL_FIELDS).
        @param value: A single value to be passed to the function, or None.
        @returns: Whatever the specified function returns.
        """
        if (function_name in dir(self.module)):
            function = getattr(self.module, function_name)
            tree = function(tree, NAMESPACES, f, value)
        return tree
    
    def labels_values(self, document):
        """Apply display_{field} functions to prep object data for the UI.
        
        Certain fields require special processing.  For example, structured data
        may be rendered in a template to generate an HTML <ul> list.
        If a "display_{field}" function is present in the ddrlocal.models.collection
        module the contents of the field will be passed to it
        
        @param document: Collection, Entity, File document object
        @returns: list
        """
        lv = []
        for f in self.module.FIELDS:
            if hasattr(document, f['name']) and f.get('form',None):
                key = f['name']
                label = f['form']['label']
                # run display_* functions on field data if present
                value = self.function(
                    'display_%s' % key,
                    getattr(document, f['name'])
                )
                lv.append( {'label':label, 'value':value,} )
        return lv
    
    def field_choices(self, field_name):
        for f in self.module.FIELDS:
            if (f['name'] == field_name) and (f['form'].get('choices')):
                return f['form']['choices']
        return None
    
    def _parse_commit(self, text):
        return text.strip().split(' ')[0]
    
    def document_commit(self, document):
        doc_metadata = getattr(document, 'object_metadata', {})
        if doc_metadata:
            document_commit_raw = doc_metadata.get('models_commit','')
            return self._parse_commit(document_commit_raw)
        return None

    def module_commit(self):
        return self._parse_commit(dvcs.latest_commit(self.path))
        
    def cmp_model_definition_commits(self, document_commit, module_commit):
        """Indicate document's model defs are newer or older than module's.
        
        Prepares repository and document/module commits to be compared
        by DDR.dvcs.cmp_commits.  See that function for how to interpret
        the results.
        Note: if a document has no defs commit it is considered older
        than the module.
        NOTE: commit may not be found in log if definitions were on a
        branch at the time the document was committed.
        
        @param document: A Collection, Entity, or File object.
        @returns: dict See DDR.dvcs.cmp_commits
        """
        return dvcs.cmp_commits(
            dvcs.repository(self.path),
            document_commit,
            module_commit
        )
    
    def cmp_model_definition_fields(self, document_json):
        """Indicate whether module adds or removes fields from document
        
        @param document_json: Raw contents of document *.json file
        @returns: dict {'added': [], 'removed': []} Lists of added,removed field names.
        """
        data = json.loads(document_json)
        # First item in list is document metadata, everything else is a field.
        document_fields = [field.keys()[0] for field in data[1:]]
        module_fields = [field['name'] for field in getattr(self.module, 'FIELDS')]
        # models.load_json() uses MODULE.FIELDS, so get list of fields
        # directly from the JSON document.
        added = [field for field in document_fields if field not in module_fields]
        removed = [field for field in module_fields if field not in document_fields]
        return {
            'added': added,
            'removed': removed
        }
