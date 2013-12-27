#!/usr/bin/env python
#
# fork of https://github.com/JohannesBuchner/imagehash
#

import Image
import numpy
import scipy.fftpack

def binary_array_to_hex(arr):
	h = 0
	s = []
	for i,v in enumerate(arr.flatten()):
		if v: h += 2**(i % 8)
		if (i % 8) == 7:
			s.append(hex(h)[2:].rjust(2, '0'))
			h = 0
	return "".join(s)

def binary_array_to_int(arr):
	return sum([2**(i % 8) for i,v in enumerate(arr.flatten()) if v])

"""
Hash encapsulation. Can be used for dictionary keys and comparisons.
"""
class ImageHash(object):
	def __init__(self, binary_array):
		self.hash = binary_array

	def __str__(self):
		return binary_array_to_hex(self.hash)

	def __repr__(self):
		return repr(self.hash)

	def __sub__(self, other):
		assert self.hash.shape == other.hash.shape, ('ImageHashes must be of the same shape!', self.hash.shape, other.hash.shape)
		return (self.hash != other.hash).sum()

	def __eq__(self, other):
		return numpy.array_equal(self.hash, other.hash)

	def __ne__(self, other):
		return not numpy.array_equal(self.hash, other.hash)

	def __hash__(self):
		return binary_array_to_int(self.hash)

def hex_to_hash(hexstr):
	l = []
	if len(hexstr) != 16:
		print hexstr
	for i in range(len(hexstr) / 2):
		#for h in hexstr[::2]:
		h = hexstr[i*2:i*2+2]
		v = int("0x" + h, 16)
		for i in range(8):
			l.append(v & 2**i > 0)
	return ImageHash(numpy.array(l))


"""
Difference Hash computation.
following http://www.hackerfactor.com/blog/index.php?/archives/529-Kind-of-Like-That.html
@image must be a PIL instance.
"""
def dhash(image, hash_size=16):
	image = image.convert("L").resize((hash_size + 1, hash_size), Image.ANTIALIAS)
	pixels = numpy.array(image.getdata(), dtype=numpy.float).reshape((hash_size + 1, hash_size))
	# compute differences
	diff = pixels[1:,:] > pixels[:-1,:]
	return ImageHash(diff)

"""
Perceptual Hash computation.
Implementation follows http://www.hackerfactor.com/blog/index.php?/archives/432-Looks-Like-It.html
@image must be a PIL instance.
"""
def phash(image, hash_size=16):
    image = image.convert("L").resize((hash_size, hash_size), Image.ANTIALIAS)
    pixels = numpy.array(image.getdata(), dtype=numpy.float).reshape((hash_size, hash_size))
    dct = scipy.fftpack.dct(pixels)
    dctlowfreq = dct[:8, 1:9]
    avg = dctlowfreq.mean()
    diff = dctlowfreq > avg
    return ImageHash(diff)

__dir__ = [dhash, phash, ImageHash]

