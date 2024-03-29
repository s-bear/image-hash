# image-hash
## Perceptual Image Hashing Utility

This utility produces perceptual hashes of images, which are short (relative to the size of the image) sequences of numbers that are similar for similar-looking images. These hashes may be used for finding duplicate or very similar images in a large dataset.

This utility has two hashing methods: a block-rank algorithm and a DCT based algorithm. Both operate on a pre-processed image, which is the input image scaled to 128x128 pixels, histogram equalized, and converted to grayscale.

The block-rank algorithm further reduces the image to 20x20 pixels, and folds the four quadrants of the image in to produce a mirror-symmetrical 10x10 image. The hash's 64 bits are determined by the central 8x8 pixels of this image. If a pixel's value is greater than half of its neighbors the corresponding bit is set, otherwise it is zero.

The DCT based algorithm computes the 2D DCT of the pre-processed image, discarding the 0-frequency and all odd-frequency components such that the final hash will be mirror-symmetrical. Each bit of the hash is set if the corresponding DCT coefficient is positive. The bits of the hash are ordered such that including fewer DCT terms produces a prefix of the larger hash. That is, the hash produced by `imghash -d1 photo.jpg` will be a prefix of that from `imghash -d2 photo.jpg`.

## Image similarity database

If built with `sqlite`, image-hash can build a [Multi-Vantage Point Tree](https://en.wikipedia.org/wiki/Vantage-point_tree) stored in a local database file. The database may be queried for images with exact or similar hashes.

Note that the database is locked to a single type of hash and will reject queries with alternate hashes specified. Support for searching DCT prefixes may be added eventually.

## Building
image-hash optionally depends on `sqlite`, `libpng` and `libjpeg`. The project is set up to use [vcpkg](https://vcpkg.io/) to collect those libraries automatically.

Build using `cmake`. The code has only been tested on Windows with MSVC 2019 and MSVC 2022.

## Usage
```
imghash [OPTIONS] [FILE [FILE ...]]
  Computes perceptual image hashes of FILEs.

  Outputs hexadecimal hash and filename for each file on a new line.
  The default algorithm (if -d is not specified) is a fixed size 64-bit block average hash, with mirror & flip tolerance.
  The DCT hash uses only even-mode coefficients, so it is mirror/flip tolerant.
  If no FILE is given, reads ppm from stdin
  OPTIONS are:
    -h, --help : print this message and exit
    -dN, --dct N : use dct hash. N may be one of 1,2,3,4 for 64,256,576,1024 bits respectively.
    -q, --quiet : don't output filename.
    -n NAME, --name NAME : specify a name for output when reading from stdin
    --db DB_PATH : use the specified database for add, query, remove, rename, and exists.
    --add : add the image to the database. If the image comes from stdin, --name must be specified.
    --query DIST LIMIT : query the database for up to LIMIT similar images within DIST distance.
    --remove NAME : remove the name from the database. No input is processed if this is specified.
    --rename OLDNAME NEWNAME : change the name of an image in the database. No input is processed if this is specified.
    --exists NAME : check if an image has been inserted into the database. No input is processed if this is specified.
  Supported file formats: 
    jpeg
    png
    ppm

```
For example:
 - `imghash -d2 photo.jpg`
 - `ffmpeg -i video.mp4 -f image2pipe -c:v ppm - | imghash -d1 > video.hashes.txt`
