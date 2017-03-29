import json
import logging
logger = logging.getLogger(__name__)

import requests

from DDR import config
from DDR import identifier


class IDServiceClient():
    """Client for interacting with ddr-idservice REST API
    
    >>> from DDR import idservice
    >>> ic = idservice.IDServiceClient()
    >>> ic.login('gjost', 'moonshapedpool')
    (200, 'OK')
    >>> ic.username
    'gjost'
    >>> ic.token
    u'9b68187429be07506dae2d1a493b74afd4ef7c35'
    
    >>> ic.resume('gjost', u'9b68187429be07506dae2d1a493b74afd4ef7c35')
    (200, 'OK')
    >>> ic.user_info()
    (200, 'OK', {u'username': u'gjost', u'first_name': u'Geoffrey', u'last_name': u'Jost', u'email': u'geoffrey.jost@densho.org'})
    
    >>> from DDR import identifier
    >>> oidentifier = identifier.Identifier('ddr-test')
    >>> ic.next_id(oidentifier, 'collection')
    201,'ddr-test-123'
    >>> cidentifier = identifier.Identifier('ddr-test-123')
    >>> ic.next_id(cidentifier, 'entity')
    201,'ddr-test-123-1'
    >>> ic.next_id(cidentifier, 'entity')
    201,'ddr-test-123-1'
    
    >>> ic.resume('gjost', u'9b68187429be07506dae2d1a493b74afd4ef7c35')
    >>> ic.logout()
    """
    debug = False
    username = None
    token = None

    def login(self, username, password):
        """Initiate a session.
        
        @param username: str
        @param password: str
        @return: int,str (status_code,reason)
        """
        self.username = username
        logging.debug('idservice.IDServiceClient.login(%s)' % (self.username))
        r = requests.post(
            config.IDSERVICE_LOGIN_URL,
            data = {'username':username, 'password':password,},
        )
        try:
            self.token = r.json().get('key')
        except ValueError:
            pass
        return r.status_code,r.reason
    
    def _auth_headers(self):
        return {'Authorization': 'Token %s' % self.token}
    
    def resume(self, token):
        """Resume a session without logging in.
        
        @param token: str
        @return: int,str (status_code,reason)
        """
        self.token = token
        status_code,reason,data = self.user_info()
        return status_code,reason
    
    def logout(self):
        """End a session.
        
        @return: int,str (status_code,reason)
        """
        logging.debug('idservice.IDServiceClient.logout() %s' % (self.username))
        r = requests.post(
            config.IDSERVICE_LOGOUT_URL,
            headers=self._auth_headers(),
        )
        return r.status_code,r.reason
    
    def user_info(self):
        """Get user information (username, first/last name, email)
        
        @return: int,str,dict (status code, reason, userinfo dict)
        """
        r = requests.get(
            config.IDSERVICE_USERINFO_URL,
            headers=self._auth_headers(),
        )
        try:
            data = json.loads(r.content)
        except ValueError:
            data = {}
        return r.status_code,r.reason,data
    
    def next_object_id(self, oidentifier, model):
        """Get the next object ID of the specified type
        
        @param oidentifier: identifier.Identifier
        @param model: str
        @return: int,str,str (status code, reason, object ID string)
        """
        logging.debug('idservice.IDServiceClient.next_object_id(%s, %s)' % (oidentifier, model))
        r = requests.post(
            config.IDSERVICE_NEXT_OBJECT_URL.format(objectid=oidentifier.id, model=model),
            headers=self._auth_headers(),
        )
        objectid = None
        if r.status_code == 201:
            objectid = r.json()['id']
            logging.debug(objectid)
        return r.status_code,r.reason,objectid
    
    def check_eids(self, cidentifier, entity_ids):
        """Given list of EIDs, indicates which are registered,unregistered.
        
        @param cidentifier: identifier.Identifier object
        @param entity_ids: list of Entity IDs!
        @returns: (status_code,reason,registered,unregistered)
        """
        logging.debug('idservice.IDServiceClient.check_eids(%s, %s entity_ids)' % (cidentifier, len(entity_ids)))
        r = requests.post(
            config.IDSERVICE_CHECKIDS_URL.format(objectid=cidentifier.id),
            headers=self._auth_headers(),
            data={'object_ids': entity_ids},
        )
        data = json.loads(r.text)
        #logging.debug(data)
        return r.status_code,r.reason,data['registered'],data['unregistered']
    
    def register_eids(self, cidentifier, entity_ids):
        """Register the specified entity IDs with the ID service
        
        @param cidentifier: identifier.Identifier object
        @param entity_ids: list of unregistered Entity IDs to add
        @returns: (status_code,reason,added_ids_list)
        """
        logging.debug('idservice.IDServiceClient.register_eids(%s, %s)' % (cidentifier, entity_ids))
        r = requests.post(
            config.IDSERVICE_REGISTERIDS_URL.format(objectid=cidentifier.id),
            headers=self._auth_headers(),
            data={'object_ids': entity_ids},
        )
        data = json.loads(r.text)
        logging.debug(data)
        return r.status_code,r.reason,data['created']
