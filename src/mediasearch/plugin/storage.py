#!/usr/bin/env python
#
# Mediasearch storage
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
    created_on: Datetime, sets on creation,
    updated_on: Datetime, sets on changes
}

media data: collections "storage_%N"
{
    _id: String(a-zA-Z0-9_-) <= reference:unique index,
    class: String(image|video|audio) <= media_class, taken from mime,
    hashes: [{method: String(dhash|phash|...), dim: Integer(8|16|32|64), repr: String(0x0-f)}],
    alike: [{ref: String<=_id, evals:[{method: String(dhash|phash|...), dim: Integer(8|16|32|64), diff: Number}]}],
    tags: [String(a-zA-Z0-9_-)],
    created_on: Datetime, sets on new hash save, i.e. on _insert,
    updated_on: Datetime, sets on tags changes, i.e. on _update,
    reliked_on: Datetime, sets when a similar media is added or removed
}
'''

import sys, os
import logging, datetime

COLLECTION_GENERAL = 'storages'
COLLECTION_PARTICULAR = 'storage_{rank}'
PROVIDER_FIELD = 'provider'
ARCHIVE_FIELD = 'archive'
CREATED_FIELD = 'created_on'
UPDATED_FIELD = 'updated_on'
RELIKED_FIELD = 'reliked_on'

class HashStorage(object):

    def __init__(self, storage=None):
        self.storage = storage
        self.is_correct = bool(self.storage)
        self.loaded_hashes = None
        self.collection_name = ''
        self.collection_set = False

    def is_correct(self):
        return self.is_correct

    def storage_set(self):
        return self.collection_set

    def set_storage(self, provider, archive, force):
        if not self.is_correct:
            return False

        # this is a simple way, lacking atomicity
        self.collection_name = ''
        self.collection_set = False
        rank = None

        try:
            collection = self.storage.db[COLLECTION_GENERAL]
            doc = collection.find_one({PROVIDER_FIELD: provider, ARCHIVE_FIELD: archive})
            if doc:
                rank = str(doc['_id'])
        except:
            self.is_correct = False
            return False

        if rank is not None:
            self.collection_set = True
        else:
            if force:
                #prepare new rank
                try:
                    collection = self.storage.db[COLLECTION_GENERAL]
                    cursor = collection.find().sort([('_id', -1)]).limit(1)
                    if not cursor.count():
                        rank = '1'
                    else:
                        doc = cursor.next()
                        rank = str(int(doc['_id']) + 1)
                    timepoint = datetime.datetime.utcnow()
                    collection.save({'_id': rank, PROVIDER_FIELD: provider, ARCHIVE_FIELD: archive, CREATED_FIELD: timepoint, UPDATED_FIELD: None})
                    self.collection_set = True
                except:
                    self.is_correct = False
                    self.collection_set = False
                    return False

        if self.collection_set:
            self.collection_name = COLLECTION_PARTICULAR.format(rank=rank)

        return True

    def get_ref_media(self, id_value):

        if not self.is_correct:
            return None
        if not self.collection_name:
            return None

        item = None
        try:
            collection = self.storage.db[self.collection_name]
            item = collection.find_one({'_id': id_value})
        except:
            self.is_correct = False
            return None

        if not item:
            return None

        return item

    def get_hashes(self, media_class, tags_with=None, tags_without=None):

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

    def load_class_hashes(self, media_class):
        if not self.is_correct:
            return False

        self.loaded_hashes = None
        if not self.collection_name:
            return False

        if not self.collection_name:
            return False

        try:
            collection = self.storage.db[self.collection_name]
            self.loaded_hashes = collection.find({'class': media_class})
        except:
            self.is_correct = False
            self.loaded_hashes = None
            return False

        return True

    def get_loaded_hash(self):
        if not self.is_correct:
            return None

        if not self.loaded_hashes:
            return None

        entry = None
        try:
            entry = self.loaded_hashes.next()
        except:
            entry = None

        if entry is None:
            try:
                self.loaded_hashes.close()
            except:
                pass
            self.loaded_hashes = None
            return None

        rv = {'ref': entry['_id'], 'hashes': entry['hashes']}
        return rv

    def save_new_media(self, store_fields, pass_mode):
        if not self.is_correct:
            return None
        if not self.collection_name:
            return None

        save_take_id = 'ref'
        save_take_string = ['class']
        save_take_list = ['hashes', 'alike', 'tags']

        id_value = store_fields[save_take_id]

        #if not pass_mode:
        if True:
            # we need to remove old references, if replacing the media; this has to be manged outside this storage connector
            try:
                collection = self.storage.db[self.collection_name]
                old_item = collection.find_one({'_id': id_value})
                if old_item:
                    return None
            except:
                self.is_correct = False
                return None

        save_data = {'_id': id_value}

        for part in save_take_string:
            save_data[part] = ''
            if store_fields[part]:
                save_data[part] = str(store_fields[part])

        for part in save_take_list:
            save_data[part] = []
            if store_fields[part]:
                part_data = store_fields[part]
                if type(part_data) is not list:
                    part_data = [part_data]
                save_data[part] = part_data

        timepoint = datetime.datetime.utcnow()

        save_data[CREATED_FIELD] = timepoint
        save_data[UPDATED_FIELD] = None
        save_data[RELIKED_FIELD] = None

        try:
            collection = self.storage.db[self.collection_name]
            collection.save(save_data)
        except:
            self.is_correct = False
            return None

        return id_value

    def append_alike_media(self, id_value, alike_item):
        # http://docs.mongodb.org/manual/tutorial/modify-documents/
        # http://docs.mongodb.org/manual/reference/operator/update/

        if not self.is_correct:
            return False
        if not self.collection_name:
            return False
        if not alike_item:
            return False

        try:
            collection = self.storage.db[self.collection_name]
            collection.update({'_id': id_value}, {'$push': {'alike': alike_item}}, upsert=False)
        except:
            # it may fail if the updated media was removed meanwhile, thus not setting the is_correct flag here
            return False

        return True

    def set_media_tags(self, id_value, tags, set_mode, pass_mode):

        if not self.is_correct:
            return False
        if not self.collection_name:
            return False

        try:
            collection = self.storage.db[self.collection_name]
        except:
            self.is_correct = False
            return False

        if not set_mode in ['set', 'add', 'del']:
            return False

        if not tags:
            tags = []
        if type(tags) is not list:
            tags = [tags]

        tag_seq = []:
        for one_tag in tags:
            if one_tag and (one_tag not in tag_seq):
                tag_seq.append(one_tag)

        if set_mode == 'set':
            try:
                collection.update({'_id': id_value}, {'$set': {'tags': tag_seq}}, upsert=False)
            except:
                return False

        if set_mode == 'add':
            if tag_seq:
                try:
                    collection.update({'_id': id_value}, {'$addToSet': {'tags': {'$each': tag_seq}}}, upsert=False)
                except:
                    return False


        if set_mode == 'del':
            if tag_seq:
                try:
                    collection.update({'_id': id_value}, {'$pullAll': {'tags': tags}}, upsert=False)
                except:
                    return False

        return True

    def delete_one_media(self, id_value, pass_mode):

        if not self.is_correct:
            return False
        if not self.collection_name:
            return False

        try:
            collection = self.storage.db[self.collection_name]
            collection.remove({'_id': id_value})
        except:
            self.is_correct = False
            return False

        return True

    def excise_alike_media(self, id_value, id_alike, pass_mode):

        if not self.is_correct:
            return False
        if not self.collection_name:
            return False

        if not id_alike:
            return False
        id_alike = str(id_alike)

        for check_char in ['\\', '\'', '"']:
            if check_char in id_alike:
                return False
        excise_spec = 'this.ref == "' + str(id_alike) + '"'

        try:
            collection = self.storage.db[self.collection_name]
            collection.update({'_id': id_value}, {'$pull': {'alike': {'$where': excise_spec}}}, upsert=False)
        except:
            return bool(pass_mode)

        return True
