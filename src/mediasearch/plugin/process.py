#!/usr/bin/env python
#
# Mediasearch
# Performs media hashing, hash storage and (perceptual) similarity search
#

import sys, os, logging, datetime
import json, tempfile, urllib2
import re, operator
from mediasearch.algs.methods import MediaHashMethods

try:
    unicode()
except:
    unicode = str

BLOCK_SIZE_GET_REMOTE = 8192
ALLOWED_SPEC = re.compile('^[\d\w_,.-]+$')
MEDIA_ENTRY_NAME = 'media'

class MediaSearch(object):
    known_media_types = {'image' : ['png', 'jpg', 'jpeg', 'pjpeg', 'gif', 'bmp', 'x-ms-bmp', 'tiff']}
    known_url_types = ['file', 'http', 'https']

    def __init__(self, base_media_path='/', tmp_dir='/tmp'):
        self.base_media_path = base_media_path
        self.tmp_dir = tmp_dir
        self.hash_methods_holder = MediaHashMethods()
        self.hash_methods = self.hash_methods_holder.get_methods()

    def _ext_download_media_file(self, media_url):
        local_file = tempfile.NamedTemporaryFile('w+b', -1, '', 'media', self.tmp_dir, False)

        try:
            block_size = BLOCK_SIZE_GET_REMOTE
            url_conn = urllib2.urlopen(media_url)
            while True:
                read_buffer = url_conn.read(block_size)
                if not read_buffer:
                    break
                local_file.write(read_buffer)
            url_conn.close()
            local_file.close()
        except:
            local_file.close()
            logging.warning('can not get remote media file: ' + str(media_url))
            return None

        local_path = local_file.name
        local_file.close()
        return local_path

    def _alg_create_hashes(self, local_path, media_class, media_type):
        prepared_hashes = []

        for cur_name in self.hash_methods:
            cur_info = self.hash_methods[cur_name]
            if not media_class in cur_info['media']:
                continue

            cur_meth = cur_info['method']
            cur_flatten = cur_info['repr']
            cur_obj = cur_info['obj']
            for cur_dim in cur_info['dims']:
                try:
                    cur_hash = cur_meth(media_type, local_path, cur_dim)
                    if cur_hash is None:
                        continue
                    cur_repr = cur_flatten(cur_hash)
                    if cur_repr is None:
                        continue
                    cur_hash = cur_obj(cur_repr)
                    if cur_hash is None:
                        continue
                    prepared_hashes.append({'method': cur_name, 'dim': cur_dim, 'obj': cur_hash, 'repr': cur_repr})
                except:
                    logging.warning('can not create media hash: ' + str(cur_name) + ', dimension: ' + str(cur_dim) + ', on: ' + str(local_path))
                    continue

        return {'class': media_class, 'evals': prepared_hashes}

    def _alg_compare_hashes(self, method_name, dimension, cmp1, cmp2):
        if not method_name in self.hash_methods:
            return None
        method_info = self.hash_methods[method_name]
        difference_threshold = method_info['lims']
        try:
            if (type(cmp1) is str) or (type(cmp1) is unicode):
                cmp1 = method_info['obj'](cmp1)
            if (type(cmp2) is str) or (type(cmp2) is unicode):
                cmp2 = method_info['obj'](cmp2)
            diff = operator.sub(cmp1, cmp2)
            if diff is None:
                return None
            dist = method_info['dist'](diff, dimension)
        except:
            logging.warning('can not compare media hashes: ' + str(method_name))
            return None

        threshold = 0
        if dimension in difference_threshold:
            threshold = difference_threshold[dimension]
        else:
            test_dim = dimension - 1
            while test_dim >= 0:
                if test_dim in difference_threshold:
                    threshold = difference_threshold[test_dim]
                    break
                test_dim -= 1

        return {'diff': diff, 'dist': dist, 'similar': (diff <= threshold)}

    def _proc_remove_media(self, media_storage, media_data, pass_mode):

        rv = media_storage.delete_one_media(media_data['ref'], True)
        if not rv:
            return False

        timepoint = datetime.datetime.utcnow()
        if media_data['alike']:
            for one_link in media_data['alike']:
                rv = media_storage.excise_alike_media(one_link['ref'], media_data['ref'], True, timepoint)
                if not rv:
                    return False

        return True

    def _proc_make_media_hash(self, media_url, media_type):

        media_type_parts = str(media_type).strip().split('/')
        if 2 != len(media_type_parts):
            return False
        if not media_type_parts[0] in self.known_media_types:
            logging.warning('unknown media class: ' + str(media_type_parts[0]))
            return False
        if not media_type_parts[1] in self.known_media_types[media_type_parts[0]]:
            logging.warning('unknown media type: ' + str(media_type))
            return False

        url_type = None
        for test_url_type in self.known_url_types:
            if media_url.startswith(test_url_type + ':'):
                url_type = test_url_type
                break

        if not url_type:
            logging.warning('unknown type of media url: ' + str(media_url))
            return False

        remove_img = False
        if 'file' != url_type:
            local_img_path = self._ext_download_media_file(media_url)
            if not local_img_path:
                return None
            remove_img = True
        else:
            local_img_path = media_url[len('file:'):]
            if local_img_path.startswith('//'):
                local_img_path = local_img_path[len('//'):]

        if not local_img_path.startswith('/'):
            local_img_path = os.path.join(self.base_media_path, local_img_path)

        media_hash = self._alg_create_hashes(local_img_path, media_type_parts[0], media_type_parts[1])

        if remove_img:
            try:
                os.unlink(local_img_path)
            except:
                pass

        return media_hash

    def _proc_compare_media_hash(self, media_storage, media_class, cmp_hash):
        found_similar = []

        media_storage.load_class_hashes(media_class)
        while True:
            oth_hash = media_storage.get_loaded_hash()
            if oth_hash is None:
                break

            oth_hash_id = oth_hash['ref']
            oth_hash_evals = {}
            for oth_hash_part in oth_hash['hashes']:
                oth_hash_key = str(oth_hash_part['method']) + '-' + str(oth_hash_part['dim'])
                if not oth_hash_key in oth_hash_evals:
                    oth_hash_evals[oth_hash_key] = []
                oth_hash_evals[oth_hash_key].append(oth_hash_part)

            cur_diffs = []
            for cmp_hash_part in cmp_hash:
                cmp_hash_key = str(cmp_hash_part['method']) + '-' + str(cmp_hash_part['dim'])
                if not cmp_hash_key in oth_hash_evals:
                    continue
                for oth_hash_part in oth_hash_evals[cmp_hash_key]:
                    cur_compared = self._alg_compare_hashes(cmp_hash_part['method'], cmp_hash_part['dim'], cmp_hash_part['obj'], oth_hash_part['repr'])
                    if (not cur_compared) or (not cur_compared['similar']):
                        continue
                    cur_diffs.append({'method': cmp_hash_part['method'], 'dim': cmp_hash_part['dim'], 'diff': str(cur_compared['diff']), 'dist': cur_compared['dist']})
            if cur_diffs:
                found_similar.append({'ref': oth_hash_id, 'evals': cur_diffs})

        return found_similar

    def _out_get_base_path(self, entry=None, provider=None, archive=None, action=None):
        use_parts = []
        action_used = True
        for one_part in [entry, provider, archive, action]:
            if one_part is None:
                action_used = False
                break
            use_parts.append(str(one_part))

        base_path = '/'.join(use_parts)
        if not action_used:
            base_path = base_path + '/'

        if not base_path.startswith('/'):
            base_path = '/' + base_path

        return base_path

    def _action_list_entries(self, storage, params):
        links = [{'entry': MEDIA_ENTRY_NAME, 'path': self._out_get_base_path(MEDIA_ENTRY_NAME)}]

        total = len(links)
        if params['offset'] is not None:
            links = links[params['offset']:]
        if params['limit'] is not None:
            links = links[:params['limit']]

        return {'items': links, 'total': total}

    def _action_list_providers(self, storage, entry, params):
        providers = storage.list_providers()

        links = []

        if not providers:
            return links

        for one_provider in providers:
            if not one_provider:
                continue
            links.append({'provider': one_provider, 'path': self._out_get_base_path(entry, one_provider)})

        total = len(links)
        if params['offset'] is not None:
            links = links[params['offset']:]
        if params['limit'] is not None:
            links = links[:params['limit']]

        return {'items': links, 'total': total}

    def _action_list_archives(self, storage, entry, provider, params):
        archives = storage.list_archives(provider)

        links = []

        if not archives:
            return links

        for one_archive in archives:
            if not one_archive:
                continue
            links.append({'archive': one_archive, 'path': self._out_get_base_path(entry, provider, one_archive)})

        total = len(links)
        if params['offset'] is not None:
            links = links[params['offset']:]
        if params['limit'] is not None:
            links = links[:params['limit']]

        return {'items': links, 'total': total}

    def _action_list_actions(self, storage, entry, provider, archive, params):

        links = []

        action_list = {
            'GET': [
                {'name': 'select', 'action': '_select'},
                {'name': 'search', 'action': '_search'}
            ],
            'POST': [
                {'name': 'create', 'action': None},
                {'name': 'drop', 'action': '_drop'},
                {'name': 'insert', 'action': '_insert'},
                {'name': 'update', 'action': '_update'},
                {'name': 'delete', 'action': '_delete'}
            ],
        }

        for method in action_list:
            for action in action_list[method]:
                action_path = self._out_get_base_path(entry, provider, archive, action['action'])
                links.append({'action': str(action['name']), 'path': action_path, 'method': str(method)})

        total = len(links)
        if params['offset'] is not None:
            links = links[params['offset']:]
        if params['limit'] is not None:
            links = links[:params['limit']]

        return {'items': links, 'total': total}

    def _action_select_media(self, storage, params):
        res = storage.get_class_media(params['ref'], params['class'], params['with'], params['without'], params['order'], params['offset'], params['limit'])
        return res

    def _action_search_media(self, storage, params):
        res = storage.get_alike_media(params['ref'], params['class'], params['with'], params['without'], params['threshold'], params['order'], params['offset'], params['limit'])
        return res

    def _action_drop_provider_archive(self, media_storage, force_mode, pass_mode):
        pass

    def _action_insert_media_hash(self, media_storage, media_fields, pass_mode):

        check_media = media_storage.get_ref_media(media_fields['ref'])
        if check_media:
            if not pass_mode:
                return False
            else:
                self._proc_remove_media(media_storage, check_media, True)

        store_fields = {}
        store_fields['ref'] = media_fields['ref']
        store_fields['tags'] = media_fields['tags']

        hashes = self._proc_make_media_hash(media_fields['url'], media_fields['mime'])
        if (not hashes) or (not hashes['evals']):
            return False

        store_fields['class'] = hashes['class']
        store_hashes = []
        for one_hash in hashes['evals']:
            store_hashes.append({'method': one_hash['method'], 'dim': one_hash['dim'], 'repr': one_hash['repr']})
        store_fields['hashes'] = store_hashes

        similar = self._proc_compare_media_hash(media_storage, hashes['class'], hashes['evals'])
        if not similar:
            similar = []
        store_fields['alike'] = similar

        timepoint = datetime.datetime.utcnow()
        media_ref = media_storage.save_new_media(store_fields, pass_mode, timepoint)
        if media_ref is None:
            return False

        for similar_item in similar:
            media_storage.append_alike_media(similar_item['ref'], {'ref': media_ref, 'evals': similar_item['evals']}, timepoint)

        return [{'ref': media_ref}]

    def _action_update_media_hash(self, media_storage, media_fields, tags_mode, pass_mode):

        if not pass_mode:
            check_media = media_storage.get_ref_media(media_fields['ref'])
            if not check_media:
                return False

        rv = media_storage.set_media_tags(media_fields['ref'], media_fields['tags'], tags_mode, pass_mode)

        if not rv:
            return None

        return [{'ref': media_fields['ref']}]

    def _action_delete_media_hash(self, media_storage, media_fields, pass_mode):

        check_media = media_storage.get_ref_media(media_fields['ref'])
        if not check_media:
            return bool(pass_mode)

        rv = self._proc_remove_media(media_storage, check_media, pass_mode)

        return bool(rv)

    def _answer_on_wrong(self, status=404, message=''):
        return (json.dumps({'_message': message}), status, {'Content-Type': 'application/json'})

    def _answer_on_items(self, status=200, meta=None, items=None):
        if not items:
            items = []
        if not meta:
            meta = {}
        if not 'total' in meta:
            meta['total'] = len(items)
        output = {
            '_meta': meta,
            '_items': items
        }

        dt_handler = lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime) or isinstance(obj, datetime.date) else json.JSONEncoder().default(obj)
        return (json.dumps(output, default=dt_handler), status, {'Content-Type': 'application/json'})

    def _answer_on_action(self, status=200, meta=None, items=None):
        if not items:
            items = []
        if not meta:
            meta = {}
        output = {
            '_meta': meta,
            '_items': items
        }

        dt_handler = lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime) or isinstance(obj, datetime.date) else json.JSONEncoder().default(obj)
        return (json.dumps(output, default=dt_handler), status, {'Content-Type': 'application/json'})

    def do_get(self, storage, entry, provider, archive, action, params):

        if not storage:
            return self._answer_on_wrong(500)

        res = None

        if ('ref' in params) and params['ref']:
            test_refs = params['ref']
            if type(test_refs) is not list:
                test_refs = [test_refs]
            for one_ref in test_refs:
                if not ALLOWED_SPEC.match(str(one_ref)):
                    logging.warning('POST request: bad ref parameter')
                    return self._answer_on_wrong(404, 'ref has to be a-zA-Z_-')

        meta = {'base': self._out_get_base_path(entry, provider, archive)}

        if action is None:
            # do basic lists

            basic_keys = ['offset', 'limit']
            params_use = {}
            for one_key in basic_keys:
                params_use[one_key] = params[one_key]

            if entry is None:
                res = self._action_list_entries(storage, params_use)
            else:
                if MEDIA_ENTRY_NAME != entry:
                    logging.warning('GET request: unknown entry')
                    return self._answer_on_wrong(404, 'unknown entry')

                if provider is None:
                    res = self._action_list_providers(storage, entry, params_use)
                else:
                    if archive is None:
                        res = self._action_list_archives(storage, entry, provider, params_use)
                    else:
                        res = self._action_list_actions(storage, entry, provider, archive, params_use)

        else:

            if MEDIA_ENTRY_NAME != entry:
                logging.warning('GET request: unknown entry')
                return self._answer_on_wrong(404, 'unknown entry')
            if (not provider) or (not archive):
                logging.warning('GET request: provider and archive have to be specified')
                return self._answer_on_wrong(404, 'provider and archive have to be specified')
            if action not in ['_select', '_search']:
                logging.warning('GET request: unknown action')
                return self._answer_on_wrong(404, 'unknown action')

            storage.set_storage(provider, archive, False)
            if not storage.is_correct():
                return self._answer_on_wrong(500)

            select_keys = ['ref', 'class', 'with', 'without', 'order', 'offset', 'limit']
            search_keys = ['ref', 'class', 'with', 'without', 'threshold', 'order', 'offset', 'limit']

            if action in ['_select']:
                if (not params['ref']) and (not params['class']):
                    logging.warning('select media: neither ref nor class provided')
                    return self._answer_on_wrong(404, 'select media: neither ref nor class provided')

                res = []
                if storage.storage_set():
                    params_use = {}
                    for one_key in select_keys:
                        params_use[one_key] = params[one_key]
                    res = self._action_select_media(storage, params_use)

            if action in ['_search']:
                if not params['ref']:
                    logging.warning('search media: ref not provided')
                    return self._answer_on_wrong(404, 'search media: ref not provided')

                res = []
                if storage.storage_set():
                    params_use = {}
                    for one_key in search_keys:
                        params_use[one_key] = params[one_key]
                    res = self._action_search_media(storage, params_use)

        if res is None:
            return self._answer_on_wrong(404)
        else:
            if 'total' in res:
                meta['total'] = res['total']
            if 'items' in res:
                res = res['items']
            return self._answer_on_items(200, meta, res)

    def do_post(self, storage, entry, provider, archive, action, media, tags_mode, pass_mode, force_mode):
        # ref: reference, id string from client media archive, possibly concatenated with archive id, etc.
        # url: local or remote path, like file:///tmp/image.png or http://some.domain.tld/dir/image.jpg
        # mime: image/png, image/jpeg, image/pjpeg, image/gif, image/bmp, image/x-ms-bmp, image/tiff
        # tags: [list_of_tags]
        #

        if not storage:
            return self._answer_on_wrong(500)
        if MEDIA_ENTRY_NAME != entry:
            logging.warning('POST request: unknown entry')
            return self._answer_on_wrong(404, 'unknown entry')
        if (not provider) or (not archive):
            logging.warning('POST request: provider and archive have to be specified')
            return self._answer_on_wrong(404, 'provider and archive have to be specified')

        if not action in [None, '_drop', '_insert', '_update', '_delete']:
            logging.warning('POST request: unknown action')
            return self._answer_on_wrong(404, 'unknown action')

        # to only force the storage creation on _insert
        # end immediately if storage is not set
        to_force_storage = False
        storage_necessary = [None, '_insert']
        if action in storage_necessary:
            to_force_storage = True

        storage.set_storage(provider, archive, to_force_storage)
        if not storage.is_correct():
            return self._answer_on_wrong(500)

        meta = {'base': self._out_get_base_path(entry, provider, archive)}

        if not storage.storage_set():
            if action in storage_necessary:
                return self._answer_on_wrong(404)
            else:
                if pass_mode:
                    return self._answer_on_action(200, meta, [])
                else:
                    return self._answer_on_wrong(404)

        res = None

        if action in ['_insert', '_update', '_delete']:
            if ('ref' in media) and media['ref']:
                if not ALLOWED_SPEC.match(str(media['ref'])):
                    logging.warning('POST request: bad ref parameter')
                    return self._answer_on_wrong(404, 'ref has to be a-zA-Z_-')

        if action in [None]:
            res = []

        if action in ['_drop']:
            res = self._action_drop_provider_archive(storage, force_mode, pass_mode)

        if action in ['_insert']:
            media_use = {'tags': media['tags']}
            for one_part in ['ref', 'url', 'mime']:
                if not media[one_part]:
                    logging.warning('insert media hash, not passed through checks: ' + str(one_part))
                    return self._answer_on_wrong(404, 'insert media hash, not passed through checks: ' + str(one_part))
                media_use[one_part] = media[one_part]
            res = self._action_insert_media_hash(storage, media_use, pass_mode)
            if not res:
                res = None

        if action in ['_update']:
            media_use = {'tags': media['tags']}
            for one_part in ['ref']:
                if not media[one_part]:
                    logging.warning('update media hash, not passed through checks: ' + str(one_part))
                    return self._answer_on_wrong(404, 'update media hash, not passed through checks: ' + str(one_part))
                media_use[one_part] = media[one_part]

            if not tags_mode:
                tags_mode = 'set'
            if not tags_mode in ['set', 'add', 'pop']:
                logging.warning('unknown tags mode: ' + str(tags_mode))
                return self._answer_on_wrong(404, 'unknown tags mode: ' + str(tags_mode))

            res = self._action_update_media_hash(storage, media_use, tags_mode, pass_mode)
            if not res:
                res = None

        if action in ['_delete']:
            media_use = {}
            for one_part in ['ref']:
                if not media[one_part]:
                    logging.warning('delete media hash, not passed through checks: ' + str(one_part))
                    return self._answer_on_wrong(404, 'delete media hash, not passed through checks: ' + str(one_part))
                media_use[one_part] = media[one_part]
            res = self._action_delete_media_hash(storage, media_use, pass_mode)
            if not res:
                res = None
            else:
                res = []

        if res is None:
            return self._answer_on_wrong(404)
        else:
            return self._answer_on_action(200, meta, res)
