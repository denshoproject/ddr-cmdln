from datetime import datetime
import json
import os

from DDR import config
from DDR import fileio
from DDR import identifier
from DDR import vocab

TESTING_BASE_DIR = os.path.join(config.TESTING_BASE_DIR, 'vocab')
if not os.path.exists(TESTING_BASE_DIR):
    os.makedirs(TESTING_BASE_DIR)

# Index.add
# Index.terms
# Index.get
# Index._get_by_term_ids
# Index._parent
# Index._children
# Index._siblings
# Index._path
# Index._format
# Index._build
# Index.load_csv
# Index.load_json
# Index.dump_csv
# Index.dump_graphviz
# Index.dump_json
# Index.dump_terms_text
# Index.dump_terms_json
# Index.menu_choices
# Index.path_choices

def test_objects():
    index = vocab.Index()
    index.add( vocab.Term( id=1, parent_id=0, title='music') )
    index.add( vocab.Term( id=2, parent_id=1, title='classical') )
    index.add( vocab.Term( id=3, parent_id=1, title='jazz') )
    index.add( vocab.Term( id=4, parent_id=1, title='electronic') )
    index.add( vocab.Term( id=5, parent_id=2, title='romantic') )
    index.add( vocab.Term( id=6, parent_id=2, title='modern') )
    index.add( vocab.Term( id=7, parent_id=3, title='traditional') )
    index.add( vocab.Term( id=8, parent_id=3, title='fusion') )
    index.add( vocab.Term( id=9, parent_id=4, title='dance') )
    index.add( vocab.Term(id=10, parent_id=4, title='experimental') )
    
    music = index.get(id=1)
    assert index._parent(music) == None
    assert index._siblings(music) == []
    assert len(index._children(music)) == 3
    
    electronic = index.get(title='electronic')
    assert index._parent(electronic).id == 1
    assert index._parent(electronic).title == 'music'
    assert len(index._siblings(electronic)) == 2
    assert index._siblings(electronic)[0].id == 2
    assert index._siblings(electronic)[0].title == 'classical'
    assert len(index._children(electronic)) == 2
    assert index._children(electronic)[0].id == 9
    assert index._children(electronic)[0].title == 'dance'
    
    experimental = index.get(title='experimental')
    assert index._parent(experimental).id == 4
    assert index._parent(experimental).title == 'electronic'
    assert len(index._siblings(experimental)) == 1
    assert index._siblings(experimental)[0].id == 9
    assert index._siblings(experimental)[0].title == 'dance'
    assert index._children(experimental) == []
    
    assert index.menu_choices() == MENU_CHOICES

# Term.__init__
# Term.__repr__
# Term._flatten
# Term.json

def test_csv():
    filename = os.path.join(TESTING_BASE_DIR, 'vocab-index-%s' % datetime.now().strftime('%Y%m%d-%H%M%S'))
    filename_csv = '%s.csv' % filename
    # prep
    terms_csv = TERMS_CSV.strip()
    with open(filename_csv, 'w') as f0:
        f0.write(terms_csv)
    # load file
    index = vocab.Index()
    with open(filename_csv, 'r') as f1:
        text = f1.read().strip()
        index.load_csv(text)
    out = index.dump_csv().strip().replace('\r', '\n').replace('\n\n', '\n')
    terms_csv = terms_csv.strip().replace('\r', '\n').replace('\n\n', '\n')
    assert out == terms_csv
    # clean up
    os.remove(filename_csv)

def test_json():
    filename = os.path.join(TESTING_BASE_DIR, 'vocab-index-%s' % datetime.now().strftime('%Y%m%d-%H%M%S'))
    filename_json = '%s.json' % filename
    # prep
    with open(filename_json, 'w') as f0:
        f0.write(json.dumps(TERMS_JSON))
    # load file
    index = vocab.Index()
    with open(filename_json, 'r') as f1:
        index.load_json(f1.read())
    out = json.loads(index.dump_json())
    assert out == TERMS_JSON
    # clean up
    os.remove(filename_json)


#Expected these headers:
#['id', '_title', 'title', 'parent_id', 'weight', 'encyc_urls', 'description', 'created', 'modified']

TERMS_CSV = """
"id","music"
"title","Music"
"description","genres of music"
"id","_title","title","parent_id","weight","created","modified","encyc_urls","description"
"1","music","Music","0","0","2015-10-29 15:52:00","2015-10-29 15:52:00","","descr"
"2","classical","Classical","1","0","2015-10-29 15:52:00","2015-10-29 15:52:00","","descr"
"3","jazz","Jazz","1","0","2015-10-29 15:52:00","2015-10-29 15:52:00","","descr"
"4","electronic","Electronic","1","0","2015-10-29 15:52:00","2015-10-29 15:52:00","","descr"
"5","romantic","Romantic","2","0","2015-10-29 15:52:00","2015-10-29 15:52:00","","descr"
"6","modern","Modern","2","0","2015-10-29 15:52:00","2015-10-29 15:52:00","","descr"
"7","traditional","Traditional","3","0","2015-10-29 15:52:00","2015-10-29 15:52:00","","descr"
"8","fusion","Fusion","3","0","2015-10-29 15:52:00","2015-10-29 15:52:00","","descr"
"9","dance","Dance","4","0","2015-10-29 15:52:00","2015-10-29 15:52:00","","descr"
"10","experimental","Experimental","4","0","2015-10-29 15:52:00","2015-10-29 15:52:00","","descr"
"""

TERMS_TEXT = """
music
NT classical, jazz, electronic

classical
BT music
NT romantic, modern
RT jazz, electronic

jazz
BT music
NT traditional, fusion
RT classical, electronic

electronic
BT music
NT dance, experimental
RT classical, jazz

romantic
BT classical
RT modern

modern
BT classical
RT romantic

traditional
BT jazz
RT fusion

fusion
BT jazz
RT traditional

dance
BT electronic
RT experimental

experimental
BT electronic
RT dance
"""

TERMS_JSON = {
    "id": "music",
    "title": "Music",
    "description": "genres of music",
    "terms": [
        {
            "id": 1,
            "parent_id": 0,
            "ancestors": [],
            "siblings": [],
            "children": [2, 3, 4],
            "created": "2015-10-29T00:00:00",
            "modified": "2015-10-29T00:00:00",
            "path": "music",
            "title": "music",
            "_title": "",
            "description": "",
            "encyc_urls": [],
            "weight": 0
        },
        {
            "id": 2,
            "parent_id": 1,
            "ancestors": [1],
            "siblings": [3, 4],
            "children": [5, 6],
            "created": "2015-10-29T00:00:00",
            "modified": "2015-10-29T00:00:00",
            "path": "music: classical",
            "title": "classical",
            "_title": "",
            "description": "",
            "encyc_urls": [],
            "weight": 0
        },
        {
            "id": 3,
            "parent_id": 1,
            "ancestors": [1],
            "siblings": [2, 4],
            "children": [7, 8],
            "created": "2015-10-29T00:00:00",
            "modified": "2015-10-29T00:00:00",
            "path": "music: jazz",
            "title": "jazz",
            "_title": "",
            "description": "",
            "encyc_urls": [],
            "weight": 0
        },
        {
            "id": 4,
            "parent_id": 1,
            "ancestors": [1],
            "siblings": [2, 3],
            "children": [9, 10],
            "created": "2015-10-29T00:00:00",
            "modified": "2015-10-29T00:00:00",
            "path": "music: electronic",
            "title": "electronic",
            "_title": "",
            "description": "",
            "encyc_urls": [],
            "weight": 0
        },
        {
            "id": 5,
            "parent_id": 2,
            "ancestors": [1, 2],
            "siblings": [6],
            "children": [],
            "created": "2015-10-29T00:00:00",
            "modified": "2015-10-29T00:00:00",
            "path": "music: classical: romantic",
            "title": "romantic",
            "_title": "",
            "description": "",
            "encyc_urls": [],
            "weight": 0
        },
        {
            "id": 6,
            "parent_id": 2,
            "ancestors": [1, 2],
            "siblings": [5],
            "children": [],
            "created": "2015-10-29T00:00:00",
            "modified": "2015-10-29T00:00:00",
            "path": "music: classical: modern",
            "title": "modern",
            "_title": "",
            "description": "",
            "encyc_urls": [],
            "weight": 0
        },
        {
            "id": 7,
            "parent_id": 3,
            "ancestors": [1, 3],
            "siblings": [8],
            "children": [],
            "created": "2015-10-29T00:00:00",
            "modified": "2015-10-29T00:00:00",
            "path": "music: jazz: traditional",
            "title": "traditional",
            "_title": "",
            "description": "",
            "encyc_urls": [],
            "weight": 0
        },
        {
            "id": 8,
            "parent_id": 3,
            "ancestors": [1, 3],
            "siblings": [7],
            "children": [],
            "created": "2015-10-29T00:00:00",
            "modified": "2015-10-29T00:00:00",
            "path": "music: jazz: fusion",
            "title": "fusion",
            "_title": "",
            "description": "",
            "encyc_urls": [],
            "weight": 0
        },
        {
            "id": 9,
            "parent_id": 4,
            "ancestors": [1, 4],
            "siblings": [10],
            "children": [],
            "created": "2015-10-29T00:00:00",
            "modified": "2015-10-29T00:00:00",
            "path": "music: electronic: dance",
            "title": "dance",
            "_title": "",
            "description": "",
            "encyc_urls": [],
            "weight": 0
        },
        {
            "id": 10,
            "parent_id": 4,
            "ancestors": [1, 4],
            "siblings": [9],
            "children": [],
            "created": "2015-10-29T00:00:00",
            "modified": "2015-10-29T00:00:00",
            "path": "music: electronic: experimental",
            "title": "experimental",
            "_title": "",
            "description": "",
            "encyc_urls": [],
            "weight": 0
        }
    ],
}


MENU_CHOICES = [
    (1, 'music'), (2, 'classical'), (3, 'jazz'), (4, 'electronic'),
    (5, 'romantic'), (6, 'modern'), (7, 'traditional'), (8, 'fusion'),
    (9, 'dance'), (10, 'experimental')
]

#def test_index_format():
#    assert False
# 
#def test_index_parse_csv_urls():
#    assert False
# 
#def test_index_dump_text():
#    assert False
# 
#def test_index_path_choices():
#    assert False
# 
#def test_term_repr():
#    assert False
# 
#def test_term_from_dict():
#    assert False
# 
#def test_term_flatten_json():
#    assert False
# 
#def test_term_csv():
#    assert False

def test_repair_topicdata():
    topics = [
        {'id': u'120', 'term': u'Activism and involvement'},
        {"id": "235", "term": "i"},
    ]
    expected = [
        {'id': u'120', 'term': u'Activism and involvement'},
        {'id': '235', 'term': u"Activism and involvement: Politics"},
    ]
    out = vocab.repair_topicdata(topics, facet=TOPICS_VOCAB)
    assert out == expected

def test_topics_choices():
    out = vocab.topics_choices(
        TOPICS_VOCAB,
        identifier.ELASTICSEARCH_CLASSES_BY_MODEL['facetterm']
    )
    print(out)
    assert out == TOPICS_CHOICES

#def test_make_tree():
#    assert False

def test_form_vocab_choices():
    out = vocab.form_vocab_choices(STATUS_VOCAB, 'status')
    assert out == STATUS_CHOICES

STATUS_VOCAB = {
    'id': 'status',
    'title': 'Status',
    'description': '',
    'terms': [
        {'id': 'inprocess', 'title': 'In Progress', 'description': ''},
        {'id': 'completed', 'title': 'Completed', 'description': ''}
    ],
}
STATUS_CHOICES = [
    ('status-completed', 'Completed'),
    ('status-inprocess', 'In Progress')
]

TOPICS_VOCAB = {
    'id': 'topics',
    'title': 'Topics',
    'description': '',
    'terms': [
        
        {
            "id": 120,
            "title": "Activism and involvement",
            "_title": "Activism and involvement [120]",
            "path": "Activism and involvement",
            "parent_id": 0,
            "ancestors": [],
            "siblings": [],
            "children": [
                235, 447, 462
            ],
            "weight": 0
        },

        {
            "id": 235,
            "title": "Politics",
            "_title": "Support from the non-Japanese American community [80] (formerly Supporters on the Outside [80])",
            "path": "Activism and involvement: Politics",
            "parent_id": 120,
            "ancestors": [120],
            "siblings": [462],
            "children": [448],
            "weight": 0,
        },

        {
            "id": 448,
            "title": "Political systems and ideologies",
            "_title": "Political systems and ideologies [448]",
            "path": "Activism and involvement: Politics: Political systems and ideologies",
            "parent_id": 235,
            "ancestors": [120, 235],
            "siblings": [],
            "children": [449, 451],
            "weight": 0,
        },

        {
            "id": 449,
            "title": "Communism",
            "_title": "Communism [449]",
            "path": "Activism and involvement: Politics: Political systems and ideologies: Communism",
            "parent_id": 448,
            "ancestors": [120, 235, 448],
            "siblings": [451],
            "children": [],
            "weight": 0,
        },

        {
            "id": 451,
            "title": "Socialism",
            "_title": "Socialism [451]",
            "path": "Activism and involvement: Politics: Political systems and ideologies: Socialism",
            "parent_id": 448,
            "ancestors": [120, 235, 448],
            "siblings": [449],
            "children": [],
            "weight": 0,
        },

        {
            "id": 447,
            "title": "Anti-war movements",
            "_title": "Anti-war movements [447]",
            "path": "Activism and involvement: Anti-war movements",
            "parent_id": 120,
            "ancestors": [120],
            "siblings": [462],
            "children": [],
            "weight": 0
        },

        {
            "id": 462,
            "title": "Movements and ideologies",
            "_title": "Movements and ideologies [462]",
            "path": "Activism and involvement: Movements and ideologies",
            "parent_id": 120,
            "ancestors": [120],
            "siblings": [235],
            "children": [463],
            "weight": 0,
        },

        {
            "id": 463,
            "title": "Feminism",
            "_title": "Feminism [463]",
            "path": "Activism and involvement: Movements and ideologies: Feminism",
            "parent_id": 462,
            "ancestors": [120, 462],
            "siblings": [],
            "children": [],
            "weight": 0
        },

    ],
}

TOPICS_CHOICES = [
    ('topics-120', 'Activism and involvement'),
    ('topics-235', 'Activism and involvement: Politics'),
    ('topics-448', 'Activism and involvement: Politics: Political systems and ideologies'),
    ('topics-449', 'Activism and involvement: Politics: Political systems and ideologies: Communism'),
    ('topics-451', 'Activism and involvement: Politics: Political systems and ideologies: Socialism'),
    ('topics-447', 'Activism and involvement: Anti-war movements'),
    ('topics-462', 'Activism and involvement: Movements and ideologies'),
    ('topics-463', 'Activism and involvement: Movements and ideologies: Feminism')
]
