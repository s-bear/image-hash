// imghash.h : Include file for standard system include files,
// or project specific include files.

#pragma once

#include <iostream>
#include <vector>
#include <cmath>
#include <cstdint>
#include <memory>
#include <cstdio>

namespace imghash {

	template<class T> struct Image;

	class Preprocess;

	Image<float> load(const std::string& fname, Preprocess& prep);

	void save(const std::string& fname, const Image<float>& img, float vmax = 1.0f);

#ifdef USE_JPEG
	bool test_jpeg(FILE* file);
	Image<float> load_jpeg(FILE* file, Preprocess& prep);
#endif
#ifdef USE_PNG
	bool test_png(FILE* file);
	Image<float> load_png(FILE* file, Preprocess& prep);
#endif
	bool test_ppm(FILE* file);
	Image<float> load_ppm(FILE* file, Preprocess& prep, bool empty_error = true);

	template<class T> T convert_pix(uint8_t p);
	template<class T> T convert_pix(uint16_t p);
	template<class T> T convert_pix(float p);

	std::vector<size_t> tile_size(size_t a, size_t b);

	template<class InT, class OutT, class TmpT>
	void resize_row(size_t in_c, size_t in_w, const InT* in, size_t out_w, OutT* out, const std::vector<size_t>& tiles, bool accumulate, std::vector<size_t>& hist)
	{
		//3 cases
		if (in_w == out_w) {
			for (size_t in_x = 0; in_x < in_w; ++in_x) {
				for (size_t c = 0; c < in_c; ++c, ++in, ++out) {
					auto p = *in;
					hist[c * 256 + convert_pix<uint8_t>(p)] += 1;
					if(accumulate) *out += convert_pix<OutT>(p);
					else *out = convert_pix<OutT>(p);
				}
			}
		}
		else if (in_w < out_w) {
			std::vector<OutT> pix(in_c, 0);
			for (size_t in_x = 0, out_x = 0; in_x < in_w; ++in_x) {
				for (size_t c = 0; c < in_c; ++c, ++in) {
					auto p = *in;
					hist[c * 256 + convert_pix<uint8_t>(p)] += 1;
					pix[c] = convert_pix<OutT>(p);
				}
				size_t tw = tiles[in_x];
				for (size_t tx = 0; tx < tw; ++tx) {
					for (size_t c = 0; c < in_c; ++c, ++out) {
						if(accumulate) *out += pix[c];
						else *out = pix[c];
					}
				}
			}
		}
		else {
			//out_w < in_w
			std::vector<TmpT> pix(in_c, TmpT(0));
			for (size_t out_x = 0, in_x = 0; out_x < out_w; ++out_x) {
				for (size_t c = 0; c < in_c; ++c) {
					pix[c] = 0;
				}
				size_t tw = tiles[out_x];
				for (size_t tx = 0; tx < tw; ++tx) {
					for (size_t c = 0; c < in_c; ++c, ++in) {
						auto p = *in;
						hist[c * 256 + convert_pix<uint8_t>(p)] += 1;
						pix[c] += convert_pix<TmpT>(p);
					}
				}
				for (size_t c = 0; c < in_c; ++c, ++out) {
					if(accumulate) *out += convert_pix<OutT>(pix[c] / tw);
					else *out = convert_pix<OutT>(pix[c] / tw);
				}
			}
		}
	}

	template<class InT, class OutT, class TmpT = OutT>
	void resize(const Image<InT>& in, Image<OutT>& out, std::vector<size_t>& hist)
	{
		if (out.channels != in.channels) {
			throw std::runtime_error("resize: in & out must have same channels");
		}

		if (hist.size() != in.channels * 256) {
			hist.resize(in.channels * 256);
		}
		std::fill(hist.begin(), hist.end(), 0);

		//generate evenly distributed tile sizes
		std::vector<size_t> tile_h;
		if (out.height > in.height) tile_h = tile_size(out.height, in.height);
		else if (in.height > out.height) tile_h = tile_size(in.height, out.height);

		std::vector<size_t> tile_w;
		if (out.width > in.width) tile_w = tile_size(out.width, in.width);
		else if (in.width > out.width) tile_w = tile_size(in.width, out.width);

		const InT* in_row = in.begin();
		OutT* out_row = out.begin();
		
		//there are 3 cases
		if (out.height == in.height) {
			for (size_t y = 0; y < out.height; ++y, in_row += in.row_size, out_row += out.row_size)
			{
				resize_row<InT, OutT, TmpT>(in.channels, in.width, in_row, out.width, out_row, tile_w, false, hist);
			}
		}
		else if (in.height < out.height) {
			std::vector<OutT> tmp(out.channels*out.width, 0);
			for (size_t in_y = 0, out_y = 0; in_y < in.height; ++in_y, in_row += in.row_size)
			{
				resize_row<InT, OutT, TmpT>(in.channels, in.width, in_row, out.width, tmp.data(), tile_w, false, hist);
				size_t th = tile_h[in_y];
				//copy the input row to each row of the output in the tile
				for (size_t ty = 0; ty < th; ++ty, out_row += out.row_size) {
					for (size_t out_x = 0, i = 0; out_x < out.width; ++out_x) {
						for (size_t c = 0; c < out.channels; ++c, ++i) {
							out_row[i] = tmp[i];
						}
					}
				}
			}
		}
		else {
			//out.height < in.height
			std::vector<TmpT> tmp(out.channels * out.width, 0);
			for (size_t out_y = 0, in_y = 0; out_y < out.height; ++out_y, out_row += out.row_size)
			{
				std::fill(tmp.begin(), tmp.end(), TmpT(0));
				size_t th = tile_h[out_y];
				for (size_t ty = 0; ty < th; ++ty, in_row += in.row_size) {
					resize_row<InT, TmpT, TmpT>(in.channels, in.width, in_row, out.width, tmp.data(), tile_w, true, hist);
				}
				for (size_t out_x = 0, i = 0; out_x < out.width; ++out_x) {
					for (size_t c = 0; c < out.channels; ++c, ++i) {
						out_row[i] = convert_pix<OutT>(tmp[i] / th);
					}
				}
			}
		}
	}

	template<class InT, class OutT, class TmpT = OutT>
	void resize(const Image<InT>& in, Image<OutT>& out)
	{
		std::vector<size_t> hist;
		resize(in, out, hist);
	}

	template<class T>
	struct Image
	{
		std::shared_ptr<T[]> data;
		size_t height, width, channels;
		size_t size, row_size;

		Image() : data(nullptr), height(0), width(0), channels(0), size(0), row_size(0)
		{}

		Image(size_t height, size_t width, size_t channels, size_t size, size_t row_size)
			: data(nullptr), height(height), width(width), channels(channels), size(size), row_size(row_size)
		{
			allocate();
		}
		Image(size_t height, size_t width, size_t channels = 1)
			: Image(height, width, channels, height* width* channels, width* channels)
		{}
		Image(const Image& other) = default;
		Image(Image&& other) = default;
		Image& operator=(const Image& other) = default;
		Image& operator=(Image&& other) = default;

		void allocate() {
			data.reset(new T[size]);
		}

		size_t index(size_t y, size_t x, size_t c) const {
			return y * row_size + x * channels + c;
		}

		T* begin() { return data.get(); }
		const T* begin() const { return data.get(); }

		T* end() { return begin() + size; }
		const T* end() const { return begin() + size; }

		T at(size_t i) const { return data[i]; }
		T& at(size_t i) { return data[i]; }

		T operator[](size_t i) const { return data[i]; }
		T& operator[](size_t i) { return data[i]; }

		T operator()(size_t y, size_t x, size_t c) const { return at(index(y, x, c)); }
		T& operator()(size_t y, size_t x, size_t c) { return at(index(y, x, c)); }
	};

	//! Preprocess image for hashing by resizing and histogram-equalizing
	class Preprocess
	{
		static constexpr size_t hist_bins = 256;

		Image<float> img;
		std::vector<size_t> hist, tile_w, tile_h;
		size_t in_h, in_w, in_c, y, i, ty;
	public:
		
		Preprocess();
		Preprocess(size_t w, size_t h);

		//by row:
		void start(size_t input_height, size_t input_width, size_t input_channels);
		
		template<class RowT>
		bool add_row(const RowT* input_row)
		{
			auto img_row = img.begin() + i;
			if (img.height == in_h) {
				resize_row<RowT, float, float>(in_c, in_w, input_row, img.width, img_row, tile_w, false, hist);
				++y;
				i += img.row_size;
			}
			else if (img.height < in_h) {
				if (ty == 0) {
					//zero the first row
					for (size_t x = 0, j = i; x < img.width; ++x) {
						for (size_t c = 0; c < img.channels; ++c, ++j) {
							img[j] = 0.0f;
						}
					}
				}
				resize_row<RowT, float, float>(in_c, in_w, input_row, img.width, img_row, tile_w, true, hist);
				++ty;
				size_t th = tile_h[y];
				if (ty >= th) {
					ty = 0;
					++y;
					i += img.row_size;
					for (size_t x = 0, j = 0; x < img.width; ++x) {
						for (size_t c = 0; c < img.channels; ++c, ++j) {
							img_row[j] /= th;
						}
					}
				}
			}
			else {
				std::vector<float> tmp(img.width * img.channels, 0);
				resize_row<RowT, float, float>(in_c, in_w, input_row, img.width, tmp.data(), tile_w, false, hist);
				size_t th = tile_h[y];
				for (ty = 0; ty < th; ++ty, ++y, i += img.row_size) {
					for (size_t x = 0, j = 0; x < img.width; ++x) {
						for (size_t c = 0; c < img.channels; ++c, ++j) {
							img[i + j] = tmp[j];
						}
					}
				}
			}
			//do we need more rows?
			return y < img.height;
		}

		Image<float> stop();

		//full-frame:
		Image<float> apply(const Image<uint8_t>& input);
	};

	//! Class for implementing image hash functions
	class Hasher
	{
	public:
		typedef std::vector<uint8_t> hash_type;
	protected:
		hash_type bytes;
		size_t bi;

		void clear();
		void append_bit(bool b);
	public:
		Hasher();
		virtual ~Hasher() {}
		virtual hash_type apply(const Image<float>& image) = 0;

		//return true if the hashes are equal up to the length of the shorter hash
		static bool match(const hash_type& h1, const hash_type& h2);
		//return true if the hashes match and are the same length
		static bool equal(const hash_type& h1, const hash_type& h2);
		
		//bitwise distance, up to the length of the shorter hash
		static uint32_t hamming_distance(const hash_type& h1, const hash_type& h2);

		static uint32_t distance(const hash_type& h1, const hash_type& h2);
	};

	//! Block-average hash
	class BlockHasher : public Hasher
	{
	public:
		hash_type apply(const Image<float>& image);
	};

	//! Discrete Cosine Transform hash
	class DCTHasher : public Hasher
	{
	protected:
		//! 1D Discrete Cosine Transform coefficient
		/*!
		\param N The number of samples.
		\param i Cosine order (DCT matrix row)
		\param j Sample index
		\return The DCT coefficient, sqrt(2/N)*cos(pi*i*(2*j + 1)/(2*N))
		*/
		static inline float coef(unsigned N, unsigned i, unsigned j)
		{
			const float d = 1.5707963267948966f / N; // pi/(2 N)
			return cos(d * i * (2 * j + 1));
		}

		//! 1D Discrete Cosine Transform matrix
		/*!
		\param N The number of samples (pixels)
		\param M The number of DCT rows, must be less than N
		\return N*M DCT matrix coefficients, in column-major order. The DC coefficients are excluded
		*/
		static std::vector<float> mat(unsigned N, unsigned M);

		//! 1D Discrete Cosine Transform matrix, even rows only
		/*!
		\param N The number of samples (pixels)
		\param M The number of DCT rows (even only), must be less than or equal to (N-1)/2
		\return N*M DCT matrix coefficients, in column-major order. The DC coefficients are excluded
		*/
		static std::vector<float> mat_even(unsigned N, unsigned M);

		//! 1D Discrete Cosine Transform matrix
		/*!
		\param N The number of samples (pixels)
		\param M The number of DCT rows, must be less than (N-1)/2 if even == true, less than N otherwise.
		\param even If true, generate only even frequency coefficients
		\return N*M DCT matrix coefficients, in column-major order. The DC coefficients are excluded
		*/
		static std::vector<float> mat(unsigned N, unsigned M, bool even);

		bool even_;
		unsigned N_, M_;
		//! 1D DCT matrix coefficients
		std::vector<float> m_;
		
	public:
		DCTHasher();

		//! Construct a DCTHash object with M frequencies
		/*!
		This Hash transforms a square image using the Discrete Cosine Transform, producing MxM bits
		of output. The DCT is computed over the full resolution of the image, and the resulting
		coefficients are ordered independently of M. That is, hashes produced from the same image
		with different values of M will have a common prefix. The ordering is by square shells,
		reading down the next column before reading across the next row:

		0 1 4 9
		2 3 5 A
		6 7 8 B
		C D E F

		Each bit of the hash corresponds to the sign of the DCT coefficients.

		\param M The number of DCT components in the output
		\param even If true, use only even frequencies, for a symmetrical hash (mirror/flip tolerant).
		*/
		DCTHasher(unsigned M, bool even);
		
		//! Apply the hash function
		hash_type apply(const Image<float>& image);
	};

}

