#!/usr/bin/env python
#
# Mediasearch
# Performs media hashing, hash storage and (perceptual) similarity search
#

import sys, os, logging
import tempfile, urllib2
import re, operator
import Image
import imagehash

BLOCK_SIZE_GET_REMOTE = 8192
ALLOWED_SPEC = re.compile('^[\d\w_-]+$')

class MediaSearch(object):
    known_media_types = {'image' : ['png', 'jpg', 'jpeg', 'pjpeg', 'gif', 'bmp', 'x-ms-bmp', 'tiff']}
    known_url_types = ['file', 'http', 'https']

    difference_threshold = {0:0, 8:8, 16:20, 32:50}
    hash_methods = {
        'image_phash': {
            'media': ['image'],
            'method': lambda x, y, z: imagehash.phash(Image.open(y), z),
            'dims': [16],
            'repr': imagehash.binary_array_to_hex,
            'obj': imagehash.hex_to_hash
        },
        'image_dhash': {
            'media': ['image'],
            'method': lambda x, y, z: imagehash.dhash(Image.open(y), z),
            'dims': [16],
            'repr': imagehash.binary_array_to_hex,
            'obj': imagehash.hex_to_hash
        }
    }

    def __init__(self, base_media_path='/', tmp_dir='/tmp'):
        self.base_media_path = base_media_path
        self.tmp_dir = tmp_dir

    def _ext_download_media_file(self, media_url):
        local_file = tempfile.NamedTemporaryFile('w+b', -1, '', 'image', self.tmp_dir, False)

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
            if not media_type in cur_info['media'][media_class]:
                continue

            cur_meth = cur_info['method']
            cur_flatten = cur_info['repr']
            for cur_dim in cur_info['dims']:
                try:
                    cur_hash = cur_meth(media_type, local_path, cur_dim)
                    if cur_hash is None:
                        continue
                    cur_repr = cur_flatten(cur_hash)
                    if cur_repr is None:
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

        try:
            if isinstance(cmp1, str):
                cmp1 = method_info['obj'](cmp1)
            if isinstance(cmp2, str):
                cmp2 = method_info['obj'](cmp2)
            diff = operator.sub(cmp1, cmp2)
        except:
            logging.warning('can not compare media hashes: ' + str(method_name))
            return None

        threshold = 0
        if dimension in self.difference_threshold:
            threshold = self.difference_threshold[dimension]
        else:
            test_dim = dimension - 1
            while test_dim >= 0:
                if test_dim in self.difference_threshold:
                    threshold = self.difference_threshold[test_dim]
                    break
                test_dim -= 1

        return {'diff': diff, 'similar': (diff <= test_dim)}

    def _proc_remove_media(self, media_storage, media_data, pass_mode):

        rv = media_storage.delete_one_media(media_data['ref'], True)
        if not rv:
            return False

        if media_data['alike']:
            for one_link in media_data['alike']:
                rv = media_storage.excise_alike_media(one_link['ref'], media_data['ref'], True)
                if not rv:
                    return False

        return True

    def _proc_make_media_hash(self, media_url, media_type):

        media_type_parts = str(media_type).strip().split('/')
        if 2 != len(media_type_parts):
            return False
        if not media_type_parts[0] in known_media_types:
            logging.warning('unknown media class: ' + str(media_type_parts[0]))
            return False
        if not media_type_parts[1] in known_media_types[media_type_parts[0]]:
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
                    cur_diffs.append({'method': cmp_hash_part['method'], 'dim': cmp_hash_part['dim'], 'diff': cur_compared['diff']})
            if cur_diffs:
                found_similar.append({'ref': oth_hash_id, 'evals': cur_diffs})

        return found_similar

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

        media_id = media_storage.save_new_media(store_fields, pass_mode)
        if media_id is None:
            return False

        for similar_item in similar:
            media_storage.append_alike_media(similar_item['ref'], {'ref': media_id, 'evals': similar_item['evals']})

    def _action_update_media_hash(self, media_storage, media_fields, tags_mode, pass_mode):

        if not pass_mode:
            check_media = media_storage.get_ref_media(media_fields['ref'])
            if not check_media:
                return False

        rv = media_storage.set_media_tags(media_fields['ref'], media_fields['tags'], tags_mode, pass_mode)

        return rv

    def _action_delete_media_hash(self, media_storage, media_fields, pass_mode):

        check_media = media_storage.get_ref_media(media_fields['ref'])
        if not check_media:
            return bool(pass_mode)

        rv = self._proc_remove_media(media_storage, check_media, pass_mode)

        return rv

    def do_get(self, storage, entry, provider, archive, action, params):

        if not storage:
            return False
        if 'media' != entry:
            return False
        if (not provider) or (not archive):
            return False

        return []

    def do_post(self, storage, entry, provider, archive, action, media, tags_mode, pass_mode):
        # ref: reference, id string from client media archive, possibly concatenated with archive id, etc.
        # url: local or remote path, like file:///tmp/image.png or http://some.domain.tld/dir/image.jpg
        # mime: image/png, image/jpeg, image/pjpeg, image/gif, image/bmp, image/x-ms-bmp, image/tiff
        # tags: [list_of_tags]
        #

        if not storage:
            return False
        if 'media' != entry:
            return False
        if (not provider) or (not archive):
            return False

        if not action in ['_insert', '_update', '_delete']:
            return False

        # to only force the storage creation on _insert
        # end immediately if storage is not set
        to_force_storage = False
        if action == 'insert':
            to_force_storage = True

        storage.set_storage(provider, archive, to_force_storage)
        if not storage.is_correct():
            return False

        if not storage.storage_set():
            return False

        res = None

        if 'ref' in media:
            if not ALLOWED_SPEC.match(media['ref']):
                return False

        if action in ['_insert']:
            media_use = {'tags': media['tags']}
            for one_part in ['ref', 'url', 'mime']:
                if not media[one_part]:
                    logging.warning('insert media hash, not passed through checks: ' + str(one_part))
                    return False
                media_use[one_part] = media[one_part]
            res = self._action_insert_media_hash(storage, media_use, pass_mode)

        if action in ['_update']:
            media_use = {'tags': media['tags']}
            for one_part in ['ref']:
                if not media[one_part]:
                    logging.warning('update media hash, not passed through checks: ' + str(one_part))
                    return False
                media_use[one_part] = media[one_part]

            if not tags_mode:
                tags_mode = 'set'
            if not tags_mode in ['set', 'add', 'del']:
                return False

            res = self._action_update_media_hash(storage, media_use, tags_mode, pass_mode)

        if action in ['_delete']:
            media_use = {}
            for one_part in ['ref']:
                if not media[one_part]:
                    logging.warning('delete media hash, not passed through checks: ' + str(one_part))
                    return False
                media_use[one_part] = media[one_part]
            res = self._action_delete_media_hash(storage, media_use, pass_mode)

        return res
