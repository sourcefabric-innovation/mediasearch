#!/usr/bin/env python
#
# Mediasearch
# Performs media hashing, hash storage and (perceptual) similarity search
#
# db: self.media_search_db
#

'''
* Database

storage data: collection "storages"
{
    _id: Integer <= internal:storage_rank:sequence,
    provider: String(a-zA-Z0-9_-) <= provider_name,
    archive: String(a-zA-Z0-9_-) <= archive_name,
    created: Datetime, sets on creation,
    updated: Datetime, sets on any change
}

media data: collections "storage_%N"
{
    _id: String(a-zA-Z0-9_-) <= reference:unique index,
    class: String(image|video|audio) <= media_class, taken from mime,
    hashes: [{method: String(dhash|phash|...), dim: Integer(8|16|32|64), repr: String(0x0-f)}],
    alike: [{media: String<=_id, evals:[{method: String(dhash|phash|...), dim: Integer(8|16|32|64), diff: Number}]}],
    tags: [String(a-zA-Z0-9_-)],
    created: Datetime, sets on new hash save, i.e. on _insert,
    updated: Datetime, sets on any change, i.e. on _insert, _update
}
'''

import sys, os, logging
import tempfile, urllib2
import operator
import Image
import imagehash

class MediaSearch(object):
    known_media_types = {'image' : ['png', 'jpg', 'jpeg', 'pjpeg', 'gif', 'bmp', 'x-ms-bmp', 'tiff']}
    known_url_types = ['file', 'http', 'https']

    # config should contain:
    #   info (site, port) to connect to MongoDB
    #   MongoDB db name for mediasearch use
    #   tmp dirs for taking remote media files
    #   may be: base media dir for taking local files
    config = {}
    difference_threshold = {0:0, 8:8, 16:20, 32:50}
    hash_methods = {
        'image_phash': {
            'media': 'image',
            'method': lambda x, y, z: imagehash.phash(Image.open(y), z),
            'dimmensions': [16],
            'repr': imagehash.binary_array_to_hex,
            'obj': imagehash.hex_to_hash
        },
        'image_dhash': {
            'media': 'image',
            'method': lambda x, y, z: imagehash.dhash(Image.open(y), z),
            'dimmensions': [16],
            'repr': imagehash.binary_array_to_hex,
            'obj': imagehash.hex_to_hash
        }
    }

    def __init__(self, storage=None):
        self.is_correct = True

        self.storage = storage
        self.base_media_path = '/'
        self.tmp_dir = '/tmp'

    def _db_get_hashes(self, media_storage, media_class, tags_with=None, tags_without=None):

        hashes = []

        #TODO: tags filtering should be delegated to MongoDB,
        # if necessary, limit to single in/out tags, or one of any/all cases
        tags_in_any = None
        tags_in_all = None
        tags_out_any = None
        tags_out_all = None
        if tags and ('in_any' in tags) and tags['in_any']:
            tags_in_any = tags['in_any']
        if tags and ('in_all' in tags) and tags['in_all']:
            tags_in_all = tags['in_all']
        if tags and ('out_any' in tags) and tags['out_any']:
            tags_out_any = tags['out_any']
        if tags and ('out_all' in tags) and tags['out_all']:
            tags_out_all = tags['out_all']

        db_collection = media_storage['collection']

        cursor = db_collection.find({'media_class': media_class})
        for entry in cursor:
            cur_tags = []
            if ('tags' in entry) and entry['tags']:
                cur_tags = entry['tags']

            if tags_in_any:
                got_in_any = False
                for test_tag in tags_in_any:
                    if test_tag in cur_tags:
                        got_in_any = True
                        break
                if not got_in_any:
                    continue

            if tags_in_all:
                got_in_all = True
                for test_tag in tags_in_all:
                    if not test_tag in cur_tags:
                        got_in_all = False
                        break
                if not got_in_all:
                    continue

            if tags_out_any:
                got_out_any = False
                for test_tag in tags_out_any:
                    if test_tag in cur_tags:
                        got_out_any = True
                        break
                if got_out_any:
                    continue

            if tags_out_all:
                got_out_all = True
                for test_tag in tags_out_all:
                    if not test_tag in cur_tags:
                        got_out_all = False
                        break
                if got_out_all:
                    continue

            hashes.append(entry)

        return hashes

    def _db_save_new_media_hash(self, media_storage, media_info):
        # http://stackoverflow.com/questions/4372797/how-do-i-update-a-mongo-document-after-inserting-it
        # http://stackoverflow.com/questions/13710770/how-to-update-values-using-pymongo
        # http://docs.mongodb.org/manual/tutorial/modify-documents/
        #
        # switch to db self.media_search_db: mongo.use(self.media_search_db)
        # coll_name = media_{storage_id}
        # coll_hnd = db[coll_name]
        # if not media_id:
        #   media_id = coll_hnd.insert(data)
        # else:
        #   data['_id'] = media_id
        #   media_id = coll_hnd.insert(data)
        #   #coll_hnd.update({'_id':media_id}, {"$set": post}, upsert=False)
        #

        save_parts = ['ref', 'class', 'hashes', 'alike', 'tags']

        db_collection = media_storage['collection']

        media_data = {}
        for cur_key in save_parts:
            media_data[cur_key] = None
            if cur_key in media_info:
                media_data[cur_key] = media_info[cur_key]

        media_id = db_collection.insert(media_data)

    def _ext_download_media_file(self, media_url):
        local_file = tempfile.NamedTemporaryFile('w+b', -1, '', 'image', self.tmp_dir, False)

        try:
            block_size = 8192
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
            if not media_class == cur_info['media']:
                continue

            cur_meth = cur_info['method']
            cur_flatten = cur_info['repr']
            for cur_dim in cur_info['dimmensions']:
                try:
                    cur_hash = cur_meth(media_type, local_path, cur_dim)
                    cur_repr = cur_flatten(cur_hash)
                    prepared_hashes.append({'method': cur_name, 'dimension': cur_dim, 'object': cur_hash, 'repr': cur_repr})
                except:
                    logging.warning('can not create media hash: ' + str(cur_name) + ', dimension: ' + str(cur_dim) + ', on: ' + str(local_path))
                    continue

        return {'media_class': media_class, 'evaluations': prepared_hashes}

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

        return {'difference': diff, 'similar': (diff <= test_dim)}

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

        if 'file' != url_type:
            local_img_path = self._ext_download_media_file(media_url)
            if not local_img_path:
                return None
        else:
            local_img_path = media_url[len('file:'):]
            if local_path.startswith('//'):
                local_path = local_path[len('//'):]
        if not local_path.startswith('/'):
            local_path = os.path.join(self.base_media_path, local_path)

        media_hash = self._alg_create_hashes(local_path, media_type_parts[0], media_type_parts[1])

        return media_hash

    def _proc_compare_media_hash(self, media_storage, cmp_hash):
        found_similar = []

        other_hashes = self._db_get_hashes(media_storage, cmp_hash['media_class'])
        for oth_hash in other_hashes:
            oth_hash_id = oth_hash['media_id']
            oth_hash_evals = {}
            for oth_hash_part in oth_hash['evaluations']:
                oth_hash_key = str(oth_hash_part['method']) + '-' + str(oth_hash_part['dimension'])
                oth_hash_evals[oth_hash_key] = oth_hash_part

            cur_diffs = []
            for cmp_hash_part in cmp_hash['evaluations']:
                cmp_hash_key = str(cmp_hash_part['method']) + '-' + str(cmp_hash_part['dimension'])
                if not cmp_hash_key in oth_hash_evals:
                    continue
                oth_hash_part = oth_hash_evals[cmp_hash_key]
                cur_compared = self._alg_compare_hashes(cmp_hash_part['method'], cmp_hash_part['dimension'], cmp_hash_part['object'], oth_hash_part['repr'])
                if (not cur_compared) or (not cur_compared['similar']):
                    continue
                cur_diffs.append({'method': cmp_hash_part['method'], 'dimension': cmp_hash_part['dimension'], 'difference': cur_compared['difference']})
            if cur_diffs:
                found_similar.append({'ref': oth_hash_id, 'eval': cur_diffs})

        return found_similar

    def _action_insert_media_hash(self, media_storage, store_fields):

        check_items = {'url': store_fields['url'], 'mime': store_fields['mime']}
        for check_key in check_items:
            if not check_items[check_key]:
                logging.warning('insert media hash, not passed through checks: ' + str(check_key))
                return False

        hashes = self._proc_make_media_hash(store_fields['url'], store_fields['mime'])
        if not hashes:
            return False

        similar = self._proc_compare_media_hash(media_storage, hashes)
        store_fields['alike'] = similar

        media_id = self._db_save_new_media_hash(media_storage, store_fields)
        if not media_id:
            return False

        for similar_item in similar:
            self._db_add_similar_media(media_storage, similar_item['_id'], {'_id': media_id, 'alike': similar_item['alike']})


    #def do_search(self, db_name, collection_name, media_id):
    #    # GET
    #    # http://localhost:9020/db_name/collection_name/_search?id=id_N
    #    # http://localhost:9020/db_name/collection_name/_search?id=id_N&tag=tag_name
    #    pass
    #    # this shall be done automatically in the do_provide method

    def do_get(self, db_name, collection_name, media_id):
        # GET
        # http://localhost:9020/db_name/collection_name/
        # ... for lists links on the image hashes
        # http://localhost:9020/db_name/collection_name/id_N
        # ... for info on a single image hash
        pass

    '''
    def do_delete(self, db_name, collection_name, media_id):
        # DELETE
        # http://localhost:9020/db_name/collection_name/id_N
        pass
        #removal can be done via POST, when _id is provided, but URL is empty

    def do_patch(self, db_name, collection_name, media_id, media_info=None):
        pass

    def do_put(self, db_name, collection_name, media_id, media_info=None):
        # PUT
        # http://localhost:9020/db_name/collection_name/id_N
        # #updates can be done via POST too, when _id/ref are provided in the data

        if not self.is_correct:
            return False

        if not db_name:
            return False

        if not collection_name:
            return False

        if not media_id:
            return False

        if not media_info:
            return False

        res = self._update_media_hash(media_id, media_info)
        return res
    '''

    def do_post(self, mongo, entry, provider, archive, action, media, pass_mode):
        # ref: reference, id string from client media archive, possibly concatenated with archive id, etc.
        # url: local or remote path, like file:///tmp/image.png or http://some.domain.tld/dir/image.jpg
        # mime: image/png, image/jpeg, image/pjpeg, image/gif, image/bmp, image/x-ms-bmp, image/tiff
        # tags: [list_of_tags]
        #

        if not mongo:
            return False

        if 'media' != entry:
            return False

        if (not provider) or (not archive):
            return False

        storage = self._db_get_storage(mongo, provider, archive)
        if not storage:
            return False

        if not action in ['_insert', '_update', 'delete']:
            return False

        res = None

        if action in ['_insert']:
            media_use = {'tags': media['tags']}
            for one_part in ['ref', 'url', 'mime']:
                if not media[one_part]:
                    return False
                media_use[one_part] = media[one_part]
            res = self._action_insert_media_hash(storage, media_use, pass_mode)

        if action in ['_update']:
            media_use = {'tags': media['tags']}
            for one_part in ['ref']:
                if not media[one_part]:
                    return False
                media_use[one_part] = media[one_part]
            res = self._action_update_media_hash(storage, media_use, pass_mode)

        if action in ['_delete']:
            media_use = {}
            for one_part in ['ref']:
                if not media[one_part]:
                    return False
                media_use[one_part] = media[one_part]
            res = self._action_delete_media_hash(storage, media_use, pass_mode)

        return res

        '''
        check_items = {'is_correct': self.is_correct, 'db_name': db_name, 'collection_name': collection_name, 'media_info': media_info}
        for check_key in check_items:
            if not check_items[check_key]:
                logging.warning('POST request, not passed through checks: ' + str(check_key))
                return False

        # either take info on the found, or create new
        media_storage = self._db_get_storage(db_name, collection_name)
        if not media_storage:
            return False

        store_fields = {'ref': None, 'url': None, 'mime': None, 'tags': None}
        for item_key in store_fields:
            if item_key in media_info:
                store_fields[item_key] = media_info[item_key]
        store_id = None
        if '_id' in media_info:
            store_id = media_info['_id']

        action = None
        if store_id:
            if not store_fields['ref']:
                action = 'delete'
            else:
                action = 'update'
        else:
            can_store = True
            for item_key in ['ref', 'url', 'mime']:
                if not store_fields[item_key]:
                    can_store = False
                    break
            if can_store:
                action = 'insert'

        if 'delete' == action:
            res = self._action_delete_media_hash(media_storage, store_id)
        elif 'update' == action:
            res = self._action_update_media_hash(media_storage, store_id, store_fields)
        elif 'insert' == action:
            res = self._action_insert_media_hash(media_storage, store_fields)
        else:
            return False

        return res
        '''
