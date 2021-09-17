# image-hash
## Perceptual Image Hashing Utility

This utility produces perceptual hashes of images, which are short (relative to the size of the image) sequences of numbers that are similar for similar-looking images. These hashes may be used for finding duplicate or very similar images in a large dataset.

This utility has two hashing methods, a block-rank algorithm and a DCT based algorithm. Both operate on a pre-processed image, which is the input image scaled to 128x128 pixels, histogram equalized, and converted to grayscale.

The block-rank algorithm further reduces the image to 20x20 pixels, and folds the four quadrants of the image in to produce a mirror-symmetrical 10x10 image. The hash's 64 bits correspond to the central 8x8 pixels of this image. Each is ranked relative to its neighbors, and if greater than half the corresponding bit is set, otherwise it is zero.

The DCT based algorithm simply computes the 2D DCT of the pre-processed image, discarding the 0-frequency and all odd-frequency components. Each bit of the hash is set if the corresponding DCT coefficient is positive. The bits of the hash are ordered such that including fewer DCT terms produces a prefix of the larger hash. That is, the hash produced by `imghash -d1 photo.jpg` will be a prefix that from `imghash -d2 photo.jpg`.

## Building
image-hash optionally depends on `libpng` and `libjpeg`. The `vcpkg.json` manifest may be used to collect those libraries automatically.

Build using `cmake`. The code has only been tested on Windows with MSVC 2019.

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
    -dN, --dct N: use dct hash. N may be one of 1,2,3,4 for 64,256,576,1024 bits respectively.
    -q, --quiet : don't output filename.
  Supported file formats: 
    jpeg
    png
    ppm

```
For example:
 - `imghash -d2 photo.jpg`
 - `ffmpeg -i video.mp4 -f image2pipe -c:v ppm - | imghash -d1 > video.hashes.txt`
