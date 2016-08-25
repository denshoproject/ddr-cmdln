# formprep_* --- Form pre-processing functions.--------------------------
#
# These functions take Python data from the corresponding Collection field
# and format it so that it can be used in an HTML form.
#

#def prep_string(data):
#def prep_datetime(data):
#def prep_list(data):
#def prep_kvlist(data):
#def prep_labelledlist(data):

def prep_listofdicts(data, keys, separator=':'):
    return formats.listofdicts_to_textnolabels(data, keys, separator)

#def prep_rolepeople(data):


# formpost_* --- Form post-processing functions ------------------------
#
# These functions take data from the corresponding form field and turn it
# into Python objects that are inserted into the Collection.
#

#def post_string(data):
#def post_datetime(data):
#def post_list(data):
#def post_kvlist(data):
#def post_labelledlist(data):

def post_listofdicts(data, keys, separators=[';', ':']):
    return formats.textnolabels_to_listofdicts(data, keys, separators)

#def post_rolepeople(data):
# 
