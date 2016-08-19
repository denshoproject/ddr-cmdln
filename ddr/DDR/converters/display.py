import os

from jinja2 import Template


# module function ------------------------------------------------------

def render(template, data):
    """Render a Jinja2 template.
    
    @param template: str Jinja2-formatted template
    @param data: dict
    """
    return Template(template).render(data=data)

def multiline_dict( template, data ):
    t = []
    for x in data:
        if type(x) == type({}):
            t.append(template.format(**x))
        else:
            t.append(x)
    return '\n'.join(t)


# display_* --- Display functions --------------------------------------
#
# These functions take Python data from the corresponding Collection field
# and format it for display.
#

#def string(data):
#def datetime(data):
#def list(data):
#def kvlist(data):
#def labelledlist(data):
#def listofdicts(data):
#def rolepeople(data):
