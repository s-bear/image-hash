// imghash.cpp : Defines the entry point for the application.
//

#include "imghash.h"

#include <vector>
#include <array>
#include <string>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <bitset>

#ifdef max
#undef max
#endif
#ifdef min
#undef min
#endif

using namespace imghash;

void print_usage() {
	std::cout << "imghash [OPTIONS] FILE [FILE ...]\n";
	std::cout << "  Computes perceptual image hashes of FILEs.\n\n";
	std::cout << "  Outputs hashes separated by spaces for each file on a new line.\n";
	std::cout << "  OPTIONS are:\n";
	std::cout << "    -h, --help : print this message and exit\n";
	std::cout << "    -b, --block : use block hash\n";
	std::cout << "    -d, --dct: use dct hash\n";
	std::cout << "    -de, --dct_even : use dct hash with even-mode DCT coefficients, for mirror/flip tolerant hashing\n";
	std::cout << "  Supported file formats: \n";
#ifdef USE_JPEG
	std::cout << "    jpeg\n";
#endif
#ifdef USE_PNG
	std::cout << "    png\n";
#endif
}

void print_version()
{
	std::cout << "imghash v0.0.1";
}

int main(int argc, const char* argv[])
{
	if (argc <= 1) {
		print_usage();
		return 1;
	}

	std::vector<std::string> files;
	bool even = false;
	bool debug = false;
	bool use_dct = false;
	bool use_block = false;
	for (size_t i = 1; i < argc; ++i) {
		auto arg = std::string(argv[i]);
		if (arg[0] == '-') {
			if (arg == "-h" || arg == "--help") {
				print_usage();
				return 0;
			}
			else if (arg == "-v" || arg == "--version") {
				print_version();
				return 0;
			}
			else if (arg == "-de" || arg == "--dct_even") use_dct = even = true;
			else if (arg == "-d" || arg == "--dct") use_dct = true;
			else if (arg == "-b" || arg == "--block") use_block = true;
			else if (arg == "--debug") debug = true;
			else {
				std::cerr << "Unknown option: " << arg << "\n";
				print_usage();
				return 1;
			}
		}
		else {
			files.emplace_back(std::move(arg));
		}
	}
	if (!(use_dct || use_block)) use_block = true;

	Preprocess prep(128, 128);
	
	auto dct = DCTHash(8, even);
	auto block = BlockHash();

	int ret = 0;
	for (const auto& file : files) {
		try {
			Image<float> img = load(file, prep);
			if (debug) {
				
				save(file + ".pgm", img, 1.0f);
			}
			uint64_t dct_hash = 0, block_hash = 0;
			if (use_dct) {
				dct_hash = dct.apply(img);
			}
			if (use_block) {
				block_hash = block.apply(img);
			}
			std::string delim = "";
			if (use_dct) {
				std::cout << delim << std::setw(16) << std::setfill('0') << std::hex << dct_hash;
				delim = " ";
			}
			if (use_block) {
				std::cout << delim << std::setw(16) << std::setfill('0') << std::hex << block_hash;
				delim = " ";
			}
			std::cout << "\n";
		}
		catch (...) {
			std::cerr << "FAILED " << file << "\n";
			ret = -1;
		}
	}
	return ret;
	
}


namespace imghash {

	template<> uint8_t convert_pix<uint8_t>(uint8_t p) { return p; }
	template<> uint16_t convert_pix<uint16_t>(uint8_t p) {
		return static_cast<uint16_t>(p << 8);
	}
	template<> float convert_pix<float>(uint8_t p) {
		return static_cast<float>(p) / 255.0f;
	}
	template<> uint16_t convert_pix<uint16_t>(uint16_t p) { return p; }
	template<> uint8_t convert_pix<uint8_t>(uint16_t p) {
		return static_cast<uint8_t>(p >> 8);
	}
	template<> float convert_pix<float>(uint16_t p) {
		return static_cast<float>(p) / 65535.0f;
	}

	template<> float convert_pix<float>(float p) { return p; }
	template<> uint8_t convert_pix<uint8_t>(float p) {
		return static_cast<uint8_t>(p * 255.9999f); //we could use nextafter(256, 0) but it might not be optimized away
	}
	template<> uint16_t convert_pix<uint16_t>(float p) {
		return static_cast<uint16_t>(p * 65535.9999f); //we could use nextafter(65536, 0) but it might not be optimized away
	}

	void save(const std::string& fname, const Image<float>& img, float vmax)
	{

		std::ofstream out(fname, std::ios::out | std::ios::binary);
		if (img.channels == 1) {
			out << "P5\n";
		}
		else if (img.channels == 3) {
			out << "P6\n";
		}
		out << img.width << " " << img.height << " " << 255 << "\n";
		float scale = nextafter(256.0f, 0.0f)/vmax;
		for (size_t y = 0, i = 0; y < img.height; ++y, i += img.row_size) {
			for (size_t x = 0, j = i; x < img.width*img.channels; ++x, ++j) {
				uint8_t p = static_cast<uint8_t>(img[j] * scale);
				out.put(p);
			}
		}
	}

	Image<float> load(const std::string& fname, Preprocess& prep)
	{
		FILE* file = fopen(fname.c_str(), "rb");
		if (file == nullptr) {
			throw std::runtime_error("Failed to open file");
		}
		if (false) {
			// this is just to make the syntax below reasonable
		}
	#ifdef USE_JPEG
		else if (test_jpeg(file)) {
			return load_jpeg(file, prep);
		}
	#endif
	#ifdef USE_PNG
		else if (test_png(file)) {
			return load_png(file, prep);
		}
	#endif
		else {
			throw std::runtime_error("Unsupported file format");
		}
	}

	uint64_t BlockHash::apply(const Image<float>& image)
	{
		const size_t N = 8;
		const size_t M = N + 2;
		Image<float> tmp(2*M, 2*M);
		resize(image, tmp);

		//fold the 4 quadrants into the top left
		for (size_t y = 0, i = 0, im = tmp.index(2*M-1,0,0);
			 y < M;
			 ++y, i += tmp.row_size, im -= tmp.row_size)
		{
			for (size_t x = 0, xm = tmp.index(0,2*M-1,0);
				 x < M;
				 ++x, --xm)
			{
				tmp[i + x] += tmp[i + xm] + tmp[im + x] + tmp[im + xm];
			}
		}

		uint64_t hash = 0;
		size_t i0 = 0;
		size_t i1 = tmp.row_size;
		size_t i2 = 2*tmp.row_size;
		for (size_t y = 0; y < N; ++y)
		{
			for (size_t x = 0;  x < N; ++x)
			{
				//we want the rank of the pixel in the center of the 3x3 neighborhood
				float p = tmp[i1 + x + 1];

				//get the surrounding 8 pixels
				auto p00 = tmp[i0 + x];
				auto p01 = tmp[i0 + x + 1];
				auto p02 = tmp[i0 + x + 2];
				auto p10 = tmp[i1 + x];
				auto p12 = tmp[i1 + x + 2];
				auto p20 = tmp[i2 + x];
				auto p21 = tmp[i2 + x + 1];
				auto p22 = tmp[i2 + x + 2];
				//calculate the rank by comparing
				int rank = (p > p00) + (p > p01) + (p > p02) + (p > p10);
				rank += (p > p12) + (p > p20) + (p > p21) + (p > p22);
				//the bit is set if p is greater than half the others
				hash <<= 1;
				if (rank >= 4) hash |= 1;
			}
			i0 = i1;
			i1 = i2;
			i2 += tmp.row_size;
		}
		return hash;
	}

	DCTHash::DCTHash(unsigned M, bool even)
		: N_(128), M_(M), even_(even), m_(mat(N_, M_, even_))
	{
		//nothing to do
	}

	DCTHash::DCTHash() : DCTHash(8, false) {}

	std::vector<float> DCTHash::mat(unsigned N, unsigned M)
	{
		if (M > N) M = N;
		std::vector<float> m;
		if (M <= 1) return m;
		//column-major order!
		m.reserve(static_cast<size_t>(N) * M);
		for (unsigned j = 0; j < N; ++j) {
			for (unsigned i = 0; i < M; ++i) {
				m.push_back(coef(N, i+1, j));
			}
		}
		return m;
	}

	std::vector<float> DCTHash::mat_even(unsigned N, unsigned M)
	{
		if (M > N/2) M = N/2;
		std::vector<float> m;
		if (M <= 1) return m;
		//column-major order!
		m.reserve(static_cast<size_t>(N) * M);
		for (unsigned j = 0; j < N; ++j) {
			for (unsigned i = 0; i < M; ++i) {
				m.push_back(coef(N, 2*(i+1), j));
			}
		}
		return m;
	}

	std::vector<float> DCTHash::mat(unsigned N, unsigned M, bool even)
	{
		if (even) return mat_even(N, M);
		else return mat(N, M);
	}

	uint64_t DCTHash::apply(const Image<float>& image)
	{
		if (image.width != image.height) return 0;
		if (N_ != image.width) {
			N_ = image.width;
			m_ = mat(N_, M_, even_);
		}
				
		/* Phase 1: Apply DCT across rows */
		Image<float> dct_1(image.height, M_);
		
		//iterate over image rows
		for (size_t y = 0, ti = 0, di = 0;
			 y < image.height;
			 ++y, ti += image.row_size, di += dct_1.row_size)
		{
			//init dct
			for (size_t u = 0, dj = di; u < dct_1.width; ++u, ++dj) {
				dct_1[dj] = 0.0f;
			}

			//iterate over image columns (reduction)
			for (size_t x = 0, tj = ti, k = 0;
				 x < image.width;
				 ++x, ++tj)
			{
				//iterate over horizontal spatial frequencies
				float p = image[tj];
				for (size_t u = 0, dj = di; u < dct_1.width; ++u, ++k, ++dj) {
					dct_1[dj] += m_[k] * p;
				}
			}
		}

		/* Phase 2: Apply DCT along columns & compute hash */
		uint64_t hash = 0;
		//iterate over vertical spatial frequencies
		for (size_t v = 0; v < M_; ++v) {
			//iterate over horizontal spatial frequencies
			for (size_t u = 0; u < M_; ++u) {
				//reduce over image rows
				float dct_uv = 0.0f;
				for (size_t y = 0, k = v, di = u; y < N_; ++y, k += M_, di += M_) {
					dct_uv += m_[k] * dct_1[di];
				}
				hash <<= 1;
				if (dct_uv > 0) hash |= 1;
			}
		}
		return hash;
	}

	std::vector<size_t> tile_size(size_t a, size_t b) 
	{
		// a > b
		//Use modified Bresenham's algorithm to distribute b groups over a items

		intptr_t D = intptr_t(b) - intptr_t(a); //the usual algorithm uses b - 2*a, but that reduces the size of the first and last bins by half
		std::vector<size_t> sizes(b, 0);
		for (size_t i = 0, j = 0; i < a; ++i) {
			sizes[j]++;
			if (D > 0) {
				++j;
				D += intptr_t(b) - intptr_t(a);
			}
			else {
				D += intptr_t(b);
			}
		}
		return sizes;
	}

	Preprocess::Preprocess(size_t w, size_t h)
		: img(h,w,3), hist(), y(0), i(0), ty(0), in_w(0), in_h(0), in_c(0)
	{
		//nothing else to do
	}

	Preprocess::Preprocess() : Preprocess(0, 0)
	{
		//nothing else to do
	}
	
	void Preprocess::start(size_t input_height, size_t input_width, size_t input_channels)
	{
		in_w = input_width;
		in_h = input_height;
		in_c = input_channels;

		if (img.height > in_h) tile_h = tile_size(img.height, in_h);
		else if (in_h> img.height) tile_h = tile_size(in_h, img.height);

		if (img.width > in_w) tile_w = tile_size(img.width, in_w);
		else if (in_w> img.width) tile_w = tile_size(in_w, img.width);

		if (hist.size() != in_c * 256) {
			hist.resize(in_c * 256);
		}
		std::fill(hist.begin(), hist.end(), 0);

		if (img.channels != in_c) {
			img = Image<float>(img.height, img.width, in_c);
		}
		y = 0;
		i = 0;
		ty = 0;		
	}

	Image<float> Preprocess::stop()
	{
		//equalization lookup table
		// cumulative sum of the normalized histogram
		std::vector<float> lut;
		lut.reserve(hist.size());
		size_t in_count = in_c * in_w * in_h;
		for (size_t c = 0, j = 0; c < in_c; ++c) {
			unsigned int sum = 0;
			for (size_t i = 0; i < hist_bins; ++i, ++j) {
				sum += hist[j];
				lut.push_back(float(sum) / in_count);
			}
		}

		Image<float> out(img.height, img.width, 1);
		//apply the equalization, storing the result in out
		for (size_t out_y = 0, out_i = 0, img_i = 0;
			 out_y < out.height;
			 ++out_y, out_i += out.row_size, img_i += img.row_size)
		{
			for (size_t out_x = 0, out_j = out_i, img_j = img_i; out_x < out.width; ++out_x, ++out_j)
			{
				float sum = 0.0f;
				for (size_t c = 0; c < img.channels; ++c, ++img_j)
				{
					auto p = img[img_j];
					sum += lut[c * hist_bins + convert_pix<uint8_t>(p)];
				}
				out[out_j] = sum;
			}
		}
		return out;
	}

	Image<float> Preprocess::apply(const Image<uint8_t>& input)
	{
		start(input.height, input.width, input.channels);
		for (const uint8_t* row = input.data.get(); add_row(row); row += input.row_size);
		return stop();
	}

}
