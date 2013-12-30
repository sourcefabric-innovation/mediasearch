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
    alike: [{ref: String<=_id, evals:[{method: String(dhash|phash|...), dim: Integer(8|16|32|64), diff: Number, dist: Number}]}],
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

    def list_providers(self):
        try:
            collection = self.storage.db[COLLECTION_GENERAL]
            providers = collection.distinct(PROVIDER_FIELD)
        except:
            self.is_correct = False
            return None

        if providers:
            providers.sort()

        return providers

    def list_archives(self, provider):
        try:
            collection = self.storage.db[COLLECTION_GENERAL]
            archives = collection.find({PROVIDER_FIELD: provider}).distinct(ARCHIVE_FIELD)
        except:
            self.is_correct = False
            return None

        if archives:
            archives.sort()

        return archives

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
                    collection.save({'_id': rank, PROVIDER_FIELD: provider, ARCHIVE_FIELD: archive, CREATED_FIELD: timepoint, UPDATED_FIELD: timepoint})
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

    def _prepare_ref_ids(self, ref_ids=None):

        if not ref_ids:
            return None

        ref_ids_use = []
        if type(ref_ids) is not list:
            ref_ids = [ref_ids]
        for one_ref in ref_ids:
            if not one_ref:
                continue
            ref_ids_use.append(str(one_ref))

        if not ref_ids_use:
            return None

        search_part = None
        if 1 == len(ref_ids_use):
            search_part = {'_id': ref_ids_use[0]}
        else:
            search_part = {'_id': {'$in': ref_ids_use}}

        return search_part

    def _prepare_tags_with(self, tags_with):

        tags_with_use = []

        if not tags_with:
            tags_with = []
        if type(tags_with) is not list:
            tags_with = [tags_with]

        for one_tag_got in tags_with:
            if not one_tag_got:
                continue
            one_tag_set = []
            if type(one_tag_got) is not list:
                one_tag_got = [one_tag_got]
            for one_tag_sub in one_tag_got:
                if not one_tag_sub:
                    continue
                one_tag_set.append({'tags': one_tag_sub})
            if not one_tag_set:
                continue
            if 1 == len(one_tag_set):
                tags_with_use.append(one_tag_set[0])
            else:
                tags_with_use.append({'$or': one_tag_set})

        rv = None
        if 1 == len(tags_with_use):
            rv = tags_with_use[0]
        if 1 < len(tags_with_use):
            rv = {'$and': tags_with_use}

        return rv

    def _prepare_tags_without(self, tags_without):

        tags_without_use = []

        if not tags_without:
            tags_without = []
        if type(tags_without) is not list:
            tags_without = [tags_without]

        for one_tag_got in tags_without:
            if not one_tag_got:
                continue
            one_tag_set = []
            if type(one_tag_got) is not list:
                one_tag_got = [one_tag_got]
            for one_tag_sub in one_tag_got:
                if not one_tag_sub:
                    continue
                one_tag_set.append({'tags': {'$not': one_tag_sub}})
            if not one_tag_set:
                continue
            if 1 == len(one_tag_set):
                tags_without_use.append(one_tag_set)
            else:
                tags_without_use.append({'$or': one_tag_set})

        rv = None
        if 1 == len(tags_without_use):
            rv = tags_without_use[0]
        if 1 < len(tags_without_use):
            rv = {'$and': tags_without_use}

        return rv

    def _prepare_order(self, order):

        order_list = []

        if order is not None:
            if type(order) is not list:
                order = [order]
                for one_sort in order:
                    one_sort = str(one_sort).lower()
                    if one_sort.startswith('ref'):
                        order_list.append(('_id', 1))
                    if one_sort.startswith('cre'):
                        order_list.append((CREATED_FIELD, -1))
                    if one_sort.startswith('upd'):
                        order_list.append((UPDATED_FIELD, -1))
                    if one_sort.startswith('rel'):
                        order_list.append((RELIKED_FIELD, -1))

        if not order_list:
            order_list = [('_id', 1)]

        return order_list

    def get_alike_media(self, ref_ids, media_class=None, tags_with=None, tags_without=None, order=None, offset=None, limit=None):
        if not self.is_correct:
            return []

        if not self.collection_set:
            return []

        search_struct = self._prepare_ref_ids(ref_ids)
        if not search_struct:
            return []

        test_refs = []
        test_evals = {}
        try:
            cursor = db_collection.find(search_struct)
            for entry in cursor:
                if ('alike' not in entry) or (not entry['alike']):
                    pass
                cur_alikes = entry['alike']
                if type(cur_alikes) is not list:
                    cur_alikes = [cur_alikes]
                for one_alike in cur_alikes:
                    if ('ref' not in one_alike) or (not one_alike['ref']):
                        continue
                    cur_ref = one_alike['ref']
                    if cur_ref not in test_refs:
                        test_refs.append(cur_ref)
                    if cur_ref not in test_evals:
                        test_evals[cur_ref] = []
                    cur_evals = []
                    if ('evals' in one_alike) and (one_alike['evals']):
                        cur_evals = one_alike['evals']
                    if type(cur_evals) is not list:
                        cur_evals = [cur_evals]
                    for one_eval in cur_evals:
                        if not one_eval:
                            continue
                        test_evals[cur_ref].append(one_eval)
        except:
            self.is_correct = False
            return []

        if not test_refs:
            return []

        search_parts = []
        if 1 == len(test_refs):
            search_parts.append({'_id': test_refs[0]})
        else:
            search_parts.append({'_id': {'$in': test_refs}})

        if media_class:
            search_parts.append({'class': media_class})

        take_with = self._prepare_tags_with(tags_with)
        if take_with:
            search_parts.append(take_with)

        take_without = self._prepare_tags_without(tags_without)
        if take_without:
            search_parts.append(take_without)

        search_struct = {}
        if 1 == len(search_parts):
            search_struct = search_parts[0]
        else:
            search_struct = {'$and': search_parts}

        order_list = self._prepare_order(order)

        take_refs = []
        take_alikes = {}
        try:
            cursor = db_collection.find(search_struct).sort(order_list)
            for entry in cursor:
                take_refs.append(entry['_id'])
                cur_take = {}
                cur_take['class'] = entry['class']
                cur_tags = []
                if ('tags' in entry) and entry['tags']:
                    cur_tags = entry['tags']
                    if type(cur_tags) is not list:
                        cur_tags = [cur_tags]
                cur_take['tags'] = cur_tags
                for one_field in [CREATED_FIELD, UPDATED_FIELD, RELIKED_FIELD]:
                    cur_take[one_field] = entry[one_field]
                take_alikes[entry['_id']] = cur_take
        except:
            self.is_correct = False
            return []

        if not take_refs:
            return []

        sort_values = {}
        eval_values = {}

        for one_ref in take_refs:
            cur_cmp = float('inf')
            if one_ref not in sort_values:
                sort_values[one_ref] = cur_cmp
            if one_ref not in eval_values:
                eval_values[one_ref] = []

            if one_ref not in test_evals:
                continue
            cur_cmp = sort_values[one_ref]

            for one_eval in test_evals[one_ref]:
                eval_values[one_ref].append(one_eval)

                if 'dist' not in one_eval:
                    continue
                try:
                    one_cmp = float(one_eval['dist'])
                except:
                    continue
                if one_cmp < cur_cmp:
                    cur_cmp = one_cmp
            sort_values[one_ref] = cur_cmp

        del(test_evals)

        take_refs.sort(key=lambda ref: sort_values[ref])

        if offset is not None:
            take_refs = take_refs[offset:]
        if limit is not None:
            take_refs = take_refs[:limit]

        output = []

        for cur_ref in take_refs:
            cur_item = {'ref': cur_ref}
            cur_item['evals'] = eval_values[cur_ref]
            for one_part in take_alikes[cur_ref]:
                cur_item[one_part] = cur_entry[one_part]
            output.append(cur_item)

        return output

    def get_class_media(self, ref_ids=None, media_class=None, tags_with=None, tags_without=None, order=None, offset=None, limit=None):
        if not self.is_correct:
            return []

        if not self.collection_set:
            return []

        search_parts = []

        ref_part = self._prepare_ref_ids(ref_ids)
        if ref_part:
            search_parts.append(ref_part)

        if media_class:
            search_parts.append({'class': media_class})

        if not search_parts:
            return []

        take_with = self._prepare_tags_with(tags_with)
        if take_with:
            search_parts.append(take_with)

        take_without = self._prepare_tags_without(tags_without)
        if take_without:
            search_parts.append(take_without)

        search_struct = {}
        if 1 == len(search_parts):
            search_struct = search_parts[0]
        else:
            search_struct = {'$and': search_parts}

        order_list = self._prepare_order(order)

        output = []

        try:
            cursor = db_collection.find(search_struct).sort(order_list)
            if offset is not None:
                cursor = cursor.skip(offset)
            if limit is not None:
                cursor = cursor.limit(limit)

            for entry in cursor:
                cur_item = {'ref': entry['_id'], 'class': entry['class']}
                cur_tags = []
                if ('tags' in entry) and entry['tags']:
                    cur_tags = entry['tags']
                    if type(cur_tags) is not list:
                        cur_tags = [cur_tags]
                cur_item['tags'] = cur_tags
                for one_field in [CREATED_FIELD, UPDATED_FIELD, RELIKED_FIELD]:
                    cur_item[one_field] = entry[one_field]

                output.append(cur_item)

        except:
            self.is_correct = False
            return []

        return output

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
        save_data[UPDATED_FIELD] = timepoint
        save_data[RELIKED_FIELD] = timepoint

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

        tag_seq = []
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

        if set_mode == 'pop':
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
