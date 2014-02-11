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
http://localhost:9020/media/provider_name/archive_name/?pass=boolean&limit=integer
http://localhost:9020/media/provider_name/archive_name/_drop?pass=boolean&force=boolean
... simple addition and removal of an archive

http://localhost:9020/media/provider_name/archive_name/_action?pass=boolean&mode=tag_setting&limit=integer
_action: _insert, _update, _delete
data: {ref,feed,url,mime,tags} for _insert, {ref,tags} for _update, {ref} for _delete
{
    ref ... reference to client-wise media id, unique, mandatory,
    feed ... for slicing the image sets (usually to high vs. low throughput feeds)
    url ... link to the media file, mandatory for _insert, not used otherwise,
    mime ... mime type, mandatory for _insert, not used otherwise,
    tags ... list of tags, _insert, _update only
}
pass: default false
_insert: whether to overwrite, if ref already exists, otherwise error returned
_update: whether to ignore non-existent ref, otherwise error returned
_delete: whether to ignore non-existent ref, otherwise error returned
limit: the count of images (per feed) to use to similarity comparisons

GET:
http://localhost:9020/media/provider_name/archive_name/_action?par1=val1&...
_action: _select, _search
parN:
ref ... case for _search, mandatory for _search: listing similar items; several values used as similar to any of them
ref ... case for _select (ref or feed mandatory for _select)
feed ... case for _search: image set slice
feed ... case for _select(ref or feed mandatory for _select): image set slice
with ... tags included, several parN means all necessary, use ","-joined values for inclusion if any tag present
without ... tags excluded, several parN means all excluded, use ","-joined vlaues for exclusion if all tags present
threshold ... distance limit, 0...1, _search only
order ... ref(default)|created|updated|reliked; similarity is the first sort criterion for _search
offset ... offset for listing
limit ... (maximal) count of items returned
'''

import os, sys, datetime, json, logging
try:
    from flask import request, Blueprint
except:
    logging.error('Flask framework is not installed')
    os._exit(1)
from mediasearch.utils.dbs import mongo_dbs
from mediasearch.plugin.process import MediaSearch
from mediasearch.plugin.storage import HashStorage

DATA_PARAM = 'data'
PASS_PARAM = 'pass'
LIMIT_PARAM = 'limit'
FORCE_PARAM = 'force'
BOOL_PARAM_TRUE = ['1', 't', 'T']
GET_PARAM_SIMPLE = ['feed', 'threshold', 'limit', 'offset']
GET_PARAM_LIST = ['ref', 'order']
GET_PARAM_LIST_DOUBLE = ['with', 'without']
GET_PARAM_SPLIT = ','
POST_PARAM_STRING = ['ref', 'feed', 'url', 'mime']
POST_PARAM_LIST = ['tags']
TAGS_MODE_PARAM = 'mode'
GET_NAT_INTEGER = ['limit', 'offset']
GET_FLOAT = ['threshold']

def _put_to_str(value):
    if value is None:
        return None
    if not value:
        return ''

    output = ''

    if not output:
        try:
            output = str(value)
        except:
            output = ''

    if not output:
        try:
            output = value.encode('utf8', 'ignore')
        except:
            output = ''

    return output

mediasearch_plugin = Blueprint('mediasearch_plugin', __name__)

@mediasearch_plugin.route('/', defaults={'entry': None, 'provider': None, 'archive': None, 'action': None}, methods=['GET'], strict_slashes=False)
@mediasearch_plugin.route('/<entry>/', defaults={'provider': None, 'archive': None, 'action': None}, methods=['GET'], strict_slashes=False)
@mediasearch_plugin.route('/<entry>/<provider>/', defaults={'archive': None, 'action': None}, methods=['GET'], strict_slashes=False)
@mediasearch_plugin.route('/<entry>/<provider>/<archive>/', defaults={'action': None}, methods=['GET'], strict_slashes=False)
@mediasearch_plugin.route('/<entry>/<provider>/<archive>/<action>/', defaults={}, methods=['GET'], strict_slashes=False)
def mediasearch_get(entry, provider, archive, action):
    '''
    Connector for GET requests
    '''

    entry = _put_to_str(entry)
    provider = _put_to_str(provider)
    archive = _put_to_str(archive)
    action = _put_to_str(action)

    media_storage = HashStorage(mongo_dbs.get_db())

    media_params = {}

    for cur_par in GET_PARAM_SIMPLE:
        media_params[cur_par] = None
        if cur_par in request.args:
            cur_val_set = request.args.get(cur_par)
            if cur_val_set:
                cur_val_set = _put_to_str(cur_val_set)
                if cur_par in GET_NAT_INTEGER:
                    try:
                        cur_val_set = int(cur_val_set)
                        if 0 > cur_val_set:
                            cur_val_set = None
                    except:
                        cur_val_set = None
                if cur_par in GET_FLOAT:
                    try:
                        cur_val_set = float(cur_val_set)
                    except:
                        cur_val_set = None
                media_params[cur_par] = cur_val_set

    for cur_par in GET_PARAM_LIST:
        media_params[cur_par] = None
        if cur_par in request.args:
            cur_val_set = []
            for got_val in request.args.getlist(cur_par):
                if got_val:
                    cur_val_set.append(_put_to_str(got_val))
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
                        cur_val.append(_put_to_str(got_subval))
                if cur_val:
                    cur_val_set.append(cur_val)
            if cur_val_set:
                media_params[cur_par] = cur_val_set

    try:
        search = MediaSearch()
        rv = search.do_get(media_storage, entry, provider, archive, action, media_params)
        return rv
    except:
        logging.error('GET request: uncaught exception')
        return (json.dumps({'_message': 'internal server error'}), 500, {'Content-Type': 'application/json'})

@mediasearch_plugin.route('/<entry>/<provider>/<archive>/', defaults={'action': None}, methods=['POST'], strict_slashes=False)
@mediasearch_plugin.route('/<entry>/<provider>/<archive>/<action>/', defaults={}, methods=['POST'], strict_slashes=False)
def mediasearch_post(entry, provider, archive, action):
    '''
    Connector for POST requests
    '''

    entry = _put_to_str(entry)
    provider = _put_to_str(provider)
    archive = _put_to_str(archive)
    action = _put_to_str(action)

    media_storage = HashStorage(mongo_dbs.get_db())

    pass_value = False
    if PASS_PARAM in request.args:
        pass_value_got = _put_to_str(request.args[PASS_PARAM])
        if pass_value_got:
            for test_start in BOOL_PARAM_TRUE:
                if pass_value_got.startswith(test_start):
                    pass_value = True
                    break

    force_value = False
    if FORCE_PARAM in request.args:
        force_value_got = _put_to_str(request.args[FORCE_PARAM])
        if force_value_got:
            for test_start in BOOL_PARAM_TRUE:
                if force_value_got.startswith(test_start):
                    force_value = True
                    break

    limit_value = None
    if LIMIT_PARAM in request.args:
        limit_value_got = _put_to_str(request.args[LIMIT_PARAM])
        if limit_value_got:
            try:
                limit_value_got = int(limit_value_got)
                if 0 > limit_value_got:
                    limit_value_got = None
            except:
                limit_value_got = None
        if limit_value_got:
            limit_value = limit_value_got

    tags_mode = None
    if TAGS_MODE_PARAM in request.args:
        tags_mode_got = _put_to_str(request.args[TAGS_MODE_PARAM])
        if tags_mode_got:
            tags_mode = tags_mode_got

    try:
        media_data = request.get_json(True, False, False)
    except:
        media_data = None

    if not media_data:
        if DATA_PARAM in request.args:
            data_value_got = _put_to_str(request.args[DATA_PARAM])
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
        media_info[cur_par] = None
        if cur_par in media_data:
            cur_val_set = media_data[cur_par]
            if cur_val_set:
                media_info[cur_par] = _put_to_str(cur_val_set)

    for cur_par in POST_PARAM_LIST:
        media_info[cur_par] = None
        if cur_par in media_data:
            cur_val_set = media_data[cur_par]
            if cur_val_set:
                cur_list = []
                if type(cur_val_set) == list:
                    for cur_val in cur_val_set:
                        cur_list.append(_put_to_str(cur_val))
                else:
                    cur_list = [_put_to_str(cur_val_set)]
                if cur_list:
                    media_info[cur_par] = cur_list

    try:
        search = MediaSearch()
        rv = search.do_post(media_storage, entry, provider, archive, action, media_info, tags_mode, pass_value, force_value, limit_value)
        return rv
    except:
        logging.error('POST request: uncaught exception')
        return (json.dumps({'_message': 'internal server error'}), 500, {'Content-Type': 'application/json'})
