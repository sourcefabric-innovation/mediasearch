#!/usr/bin/env python
#
# Mediasearch
#
# config should contain:
#   info (site, port) to connect to MongoDB
#   MongoDB db name for mediasearch use
#   tmp dirs for taking remote media files
#   may be: base media dir for taking local files
#

'''
* Requests

GET:
http://localhost:9020/
returns list of entry points; by now, the only used is "media"
http://localhost:9020/media/
returns list of providers
http://localhost:9020/media/provider_name/
returns list of archives
http://localhost:9020/media/provider_name/archive_name/
returns list of actions

POST:
http://localhost:9020/media/provider_name/archive_name/_action?pass=boolean
_action: _insert, _update, _delete
data: {ref,url,mime,tags} for _insert, {ref,tags} for _update, {ref} for _delete
{
    ref ... reference to client-wise media id, unique, mandatory,
    url ... link to the media file, mandatory for _insert, not used otherwise,
    mime ... mime type, mandatory for _insert, not used otherwise,
    tags ... list of tags, _insert, _update only
}
pass: default false
_insert: whether to overwrite, if ref already exists, otherwise error returned
_update: whether to ignore non-existent ref, otherwise error returned
_delete: whether to ignore non-existent ref, otherwise error returned

GET:
http://localhost:9020/media/provider_name/archive_name/_action?par1=val1&...
_action: _search
parN:
ref ... if any ref specified, listing is on similar items only; several values used as similar to any of them
with ... tags included, several parN means all necessary, use ","-joined values for inclusion if any tag present
without ... tags excluded, several parN means all excluded, use ","-joined vlaues for exclusion if all tags present
limit ... (maximal) count of items returned
offset ... offset for listing
order ... ref(default)|created|updated; similarity as the first sort criterion if any ref specified
'''

import datetime, json
from flask import request, Blueprint
from mediasearch.app.dbs import mongo_dbs, mediasearch_db_key
from mediasearch.plugin.process import MediaSearch
from mediasearch.plugin.storage import HashStorage

DATA_PARAM = 'data'
PASS_PARAM = 'pass'
PASS_PARAM_TRUE = ['1', 't', 'T']
GET_PARAM_SIMPLE = ['limit', 'offset']
GET_PARAM_LIST = ['ref', 'order']
GET_PARAM_LIST_DOUBLE = ['with', 'without']
GET_PARAM_SPLIT = ','
POST_PARAM_STRING = ['ref', 'url', 'mime']
POST_PARAM_LIST = ['tags']
TAGS_MODE_PARAM = 'mode'

mediasearch_plugin = Blueprint('mediasearch_plugin', __name__)

@mediasearch_plugin.route('/', defaults={'entry': None, 'provider': None, 'archive': None, 'action': None}, methods=['GET'])
@mediasearch_plugin.route('/<entry>/', defaults={'provider': None, 'archive': None, 'action': None}, methods=['GET'])
@mediasearch_plugin.route('/<entry>/<provider>/', defaults={'archive': None, 'action': None}, methods=['GET'])
@mediasearch_plugin.route('/<entry>/<provider>/<archive>/', defaults={'action': None}, methods=['GET'])
@mediasearch_plugin.route('/<entry>/<provider>/<archive>/<action>', defaults={}, methods=['GET'])
def mediasearch_get(entry, provider, archive, action):
    '''
    Connector for GET requests
    '''

    media_storage = HashStorage(mongo_dbs[mediasearch_db_key])

    media_params = {}

    for cur_par in GET_PARAM_SIMPLE:
        media_params[cur_par] = None
        if cur_par in request.args:
            cur_val_set = request.args.get(cur_par)
            if cur_val_set:
                media_params[cur_par] = cur_val_set

    for cur_par in GET_PARAM_LIST:
        media_params[cur_par] = None
        if cur_par in request.args:
            cur_val_set = []
            for got_val in request.args.getlist(cur_par):
                if got_val:
                    cur_val_set.append(got_val)
            if cur_val_set:
                media_params[cur_par] = cur_val_set

    for cur_par in GET_PARAM_LIST_DOUBLE:
        media_params[cur_par] = None
        if cur_par in request.args:
            cur_val_set = []
            for got_val in request.args.getlist(cur_par):
                cur_val = []
                for got_subval in got_val.split(GET_PARAM_SPLIT):
                    if got_subval:
                        cur_val.append(got_subval)
                if cur_val:
                    cur_val_set.append(cur_val)
            if cur_val_set:
                media_params[cur_par] = cur_val_set

    search = mediasearch.MediaSearch()
    rv = search.do_get(media_storage, entry, provider, archive, action, media_params)

    return rv

@mediasearch_plugin.route('/<entry>/<provider>/<archive>/<action>', defaults={}, methods=['POST'])
def mediasearch_post(entry, provider, archive, action):
    '''
    Connector for POST requests
    '''

    media_storage = HashStorage(mongo_dbs[mediasearch_db_key])

    pass_value = False
    if PASS_PARAM in request.args:
        pass_value_got = str(request.args[PASS_PARAM])
        if pass_value_got:
            for test_start in PASS_PARAM_TRUE:
                if pass_value_got.startswith(test_start):
                    pass_value = True
                    break

    tags_mode = None
    if TAGS_MODE_PARAM in request.args:
        tags_mode_got = str(request.args[TAGS_MODE_PARAM])
        if tags_mode_got:
            tags_mode = tags_mode_got

    try:
        media_data = request.get_json(True, False, False)
        if type(media_info) == str:
            media_data = json.loads(media_data)
    except:
        media_data = None

    if not media_data:
        if DATA_PARAM in request.args:
            data_value_got = str(request.args[DATA_PARAM])
            if data_value_got:
                try:
                    media_data = json.loads(data_value_got)
                except:
                    media_data = None

    if not media_data:
        media_data = {}

    if type(media_data) != dict:
        media_data = {}

    media_info = {}

    for cur_par in POST_PARAM_STRING:
        media_params[cur_par] = None
        if cur_par in media_data:
            cur_val_set = media_data[cur_par]
            if cur_val_set:
                media_info[cur_par] = str(cur_val_set)

    for cur_par in POST_PARAM_LIST:
        media_params[cur_par] = None
        if cur_par in media_data:
            cur_val_set = media_data[cur_par]
            if cur_val_set:
                if type(cur_val_set) == str:
                    cur_val_set = [cur_val_set]
                if type(cur_val_set) == list:
                    cur_list = []
                    for cur_val in cur_val_set:
                        if type(cur_val) == str:
                            cur_list.append(cur_val)
                    if cur_list:
                        media_info[cur_par] = str(cur_val_set)

    search = mediasearch.MediaSearch()
    rv = search.do_post(media_storage, entry, provider, archive, action, media_info, tags_mode, pass_value)

    return rv
