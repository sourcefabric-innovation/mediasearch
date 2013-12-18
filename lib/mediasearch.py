#!/usr/bin/env python
#
# Performs media hashing, hash storage and (perceptual) similarity search
#
# db: self.media_search_db
# collections:
#   storages: {id, storage_id, db_name (some_name, e.g. client name), collection_name (some_name, e.g. public_photos),
#              media_class (image, video, audio), hash_type (dhash, phash), norm_dim (8/16/32/64)}
#   storage_N: {id, media_id (remote_archive_id), media_url, media_type (png/jpg/...), hash_string, tags:[list_of_tags]}
#
# http://localhost:9020/db_name/collection_name/_action?data=data
#

import sys, os, logging
import tempfile, urllib2
import operator
import Image
import imagehash

class MediaSearch(object):
    known_media_types = ['png', 'jpg', 'bmp', 'gif']
    known_url_types = ['file', 'http', 'https']

    # config should contain:
    #   info (site, port) to connect to MongoDB
    #   MongoDB db name for mediasearch use
    #   tmp dirs for taking remote media files
    #   may be: base media dir for taking local files
    config = {}

    def __init__(self, hash_type='dhash', norm_size=16, storage=None):
        self.is_correct = True

        # the hash_type, hash_size, should be taken from the collection info
        self.hash_type = hash_type.lower()
        self.norm_size = norm_size
        self.storage = storage
        self.base_media_path = '/'
        self.tmp_dir = '/tmp'

        self.hash_fnc = None
        if 'dhash' == self.hash_type:
            self.hash_fnc = imagehash.dhash
        elif 'phash' == self.hash_type:
            self.hash_fnc = imagehash.phash
        else:
            logging.error('unknown hash function: ' + str(self.hash_type))
            self.is_correct = False


    def _download_media_file(self, media_url, media_type):
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

    def _make_media_hash(self, media_storage, media_url, media_type):
        if media_type not in known_media_types:
            logging.warning('unknown media type: ' + str(media_type))
            return False

        if not media_url:
            logging.warning('no media url')
            return False

        url_type = None
        for test_url_type in known_url_types:
            if media_url.startswith(test_url_type + ':'):
                url_type = test_url_type
                break

        if not url_type:
            logging.warning('unknown type of media url: ' + str(media_url))
            return False

        if 'file' != url_type:
            local_img_path = self._download_media_file()
            if not local_img_path:
                return None
        else:
            local_img_path = media_url[len('file:'):]
            if local_path.startswith('//'):
                local_path = local_path[len('//'):]
        if not local_path.startswith('/'):
            local_path = os.path.join(self.base_media_path, local_path)

        try:
            img = Image.open(local_img_path)
        except:
            logging.warning('can not open media file: ' + str(local_img_path))
            return None

        try:
            media_hash = self.hash_fnc(img, self.norm_size)
        except:
            logging.warning('can not create media hash')
            return None


        ###
        if not media_storage['storage_id']:
            media_storage['media_class'] = media_hash['media_class']
        else:
            if media_storage['media_class'] != media_hash['media_class']:
                return None
        ###


        return media_hash

    def _store_media_hash(self, media_storage, media_hash, media_id, media_info):
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

        pass


    def do_remove(self, db_name, collection_name, media_id):
        # DELETE
        # http://localhost:9020/db_name/collection_name/id_N
        pass

    def do_provide(self, db_name, collection_name, media_id):
        # GET
        # http://localhost:9020/db_name/collection_name/
        # http://localhost:9020/db_name/collection_name/id_N
        pass


    def do_search(self, db_name, collection_name, media_id):
        # GET
        # http://localhost:9020/db_name/collection_name/_search?id=id_N
        # http://localhost:9020/db_name/collection_name/_search?id=id_N&tag=tag_name
        pass


    def do_store(self, db_name, collection_name, media_id=None, media_info=None):
        # POST
        # http://localhost:9020/db_name/collection_name/
        # PUT
        # http://localhost:9020/db_name/collection_name/id_N
        # data={
        #   id: null or id from client media archive
        #   url: local or remote path, like file:///tmp/image.png or http://some.domain.tld/dir/image.jpg
        #   type: png, jpg, gif, bmp
        #   tags: [list_of_tags]
        # }
        #

        if not self.is_correct:
            return False

        if not media_info:
            return False

        for item_key in ['id', 'url', 'type', 'tags']:
            if not item_key in media_info:
                return False

        # either take info on the found, or create new, with no info filled in
        media_storage = self._get_storage(db_name, collection_name)
        if not media_storage['storage_id']:
            media_storage['hash_type'] = self.default['hash_type']
            media_storage['norm_dim'] = self.default['norm_dim']

        media_hash = self._make_media_hash(media_storage, media_url, media_type) # returns structure with hash, and original file info
        if not media_hash:
            return None

        if not media_storage['storage_id']:
            self._create_storage(media_storage)

        res = self._store_media_hash(media_storage, media_hash, media_id, media_info)

        pass


