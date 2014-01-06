#!/usr/bin/env python
#
# Mediasearch
# Performs media hashing, hash storage and (perceptual) similarity search
#

import sys, os, logging
import Image
from mediasearch.algs import imagehash

class MediaHashMethods(object):
    def __init__(self):
        self.hash_methods = {
            'image_phash': {
                'media': ['image'],
                'method': lambda x, y, z: imagehash.phash(Image.open(y), z),
                'dist': lambda x, y: (float(x) / (y * y)),
                'dims': [8, 16],
                'repr': lambda x: str(imagehash.binary_array_to_hex(x.hash)),
                'obj': imagehash.hex_to_hash,
                # setting weaker thresholds during testing
                #'lims': {0:0, 4:2, 8:10, 16:40, 32:160}
                'lims': {0:0, 4:4, 8:16, 16:64, 32:256}
            },
            'image_dhash': {
                'media': ['image'],
                'method': lambda x, y, z: imagehash.dhash(Image.open(y), z),
                'dist': lambda x, y: (float(x) / (y * y)),
                'dims': [8, 16],
                'repr': lambda x: str(imagehash.binary_array_to_hex(x.hash)),
                'obj': imagehash.hex_to_hash,
                # setting weaker thresholds during testing
                #'lims': {0:0, 4:2, 8:10, 16:40, 32:160}
                'lims': {0:0, 4:4, 8:16, 16:64, 32:256}
            }
        }

    def get_methods(self):
        return self.hash_methods

