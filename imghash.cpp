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

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif


#ifdef max
#undef max
#endif
#ifdef min
#undef min
#endif

using namespace imghash;

void print_usage() {
	std::cout << "imghash [OPTIONS] [FILE [FILE ...]]\n";
	std::cout << "  Computes perceptual image hashes of FILEs.\n\n";
	std::cout << "  Outputs hexadecimal hash and filename for each file on a new line.\n";
	std::cout << "  The default algorithm (if -d is not specified) is a fixed size 64-bit block average hash, with mirror & flip tolerance.\n";
	std::cout << "  The DCT hash uses only even-mode coefficients, so it is mirror/flip tolerant.\n";
	std::cout << "  If no FILE is given, reads ppm from stdin\n";
	std::cout << "  OPTIONS are:\n";
	std::cout << "    -h, --help : print this message and exit\n";
	std::cout << "    -dN, --dct N: use dct hash. N may be one of 1,2,3,4 for 64,256,576,1024 bits respectively.\n";
	//std::cout << "    -x : binary output.\n";
	std::cout << "    -q, --quiet : don't output filename.\n";
	std::cout << "  Supported file formats: \n";
#ifdef USE_JPEG
	std::cout << "    jpeg\n";
#endif
#ifdef USE_PNG
	std::cout << "    png\n";
#endif
	std::cout << "    ppm\n";
}

void print_version()
{
	std::cout << "imghash v0.0.1";
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
		try {
			Image<float> img;
			if (test_ppm(file)) {
				img = load_ppm(file, prep);
			}
		#ifdef USE_JPEG
			else if (test_jpeg(file)) {
				img = load_jpeg(file, prep);
			}
		#endif
		#ifdef USE_PNG
			else if (test_png(file)) {
				img = load_png(file, prep);
			}
		#endif
			else {
				throw std::runtime_error("Unsupported file format");
			}
			fclose(file);
			return img;
		}
		catch (std::exception& e) {
			fclose(file);
			throw e;
		}
	}

	bool test_ppm(FILE* file) {
		unsigned char magic[2] = { 0 };
		auto off = ftell(file);
		fread(magic, sizeof(unsigned char), 2, file);
		fseek(file, off, SEEK_SET);
		return (magic[0] == 'P') && (magic[1] == '6');
	}

	Image<float> load_ppm(FILE* file, Preprocess& prep, bool empty_error)
	{
		
		// 1. Magic number
		// 2. Whitespace
		// 3. Width, ASCII decimal
		// 4. Whitespace
		// 5. Height, ASCII decimal
		// 6. Whitespace
		// 7. Maxval, ASCII decimal
		// 8. A single whitespace character
		// 9. Raster (width x height x 3) bytes, x2 if maxval > 255, MSB first
		// At any point before 8, # begins a comment, which persists until the next newline or carriage return

		const size_t maxsize = 0x40000000; // 1 GB
		const size_t bufsize = 256;
		char buffer[bufsize] = { 0 };

		auto parse_space = [&](int c) {
			bool comment = ((char)c == '#');
			while (isspace(c) || (comment && c != EOF)) {
				c = fgetc(file);
				if (comment) {
					if ((char)c == '\r' || (char)c == '\n') comment = false;
				}
				else {
					if ((char)c == '#') comment = true;
				}
			}
			if (c == EOF) {
				throw std::runtime_error("PPM: Unexpected EOF");
			}
			return c;
		};
		auto parse_size = [&](int c, size_t& x) {
			size_t i = 0;
			while (isdigit(c)) {
				buffer[i++] = (char)c;
				if (i >= bufsize - 1) {
					throw std::runtime_error("PPM: Buffer overflow");
				}
				c = fgetc(file);
			}
			if (c == EOF) {
				throw std::runtime_error("PPM: Unexpected EOF");
			}
			buffer[i] = 0;
			x = atoll(buffer);
			return c;
		};
				
		//1. Magic number
		if (fread(buffer, sizeof(char), 2, file) == 0) {
			//empty file / end of stream
			if (empty_error) throw std::runtime_error("PPM: Empty file");
			else return Image<float>();
		}
		
		if (buffer[0] != 'P' || buffer[1] != '6') {
			throw std::runtime_error(std::string("PPM: Invalid file (") + buffer + ")");
		}

		// 2. Whitespace or comment
		int c = fgetc(file);
		c = parse_space(c);
		
		// 3. Width, ASCII decimal
		size_t width = 0;
		c = parse_size(c, width);
		
		// 4. Whitespace
		c = parse_space(c);
		
		// 5. Height, ASCII decimal
		size_t height = 0;
		c = parse_size(c, height);
		
		// 6. Whitespace
		c = parse_space(c);
		
		// 7. Maxval, ASCII decimal
		size_t maxval = 0;
		c = parse_size(c, maxval);
		
		//any final comment
		bool comment = ((char)c == '#');
		while (comment && c != EOF) {
			c = fgetc(file);
			if (c == '\r' || c == '\n') comment = false;
		}
		if (c == EOF) {
			throw std::runtime_error("PPM: Unexpected EOF");
		}
		// 8. A single whitespace character
		if (!isspace(c)) {
			throw std::runtime_error("PPM: No whitespace after maxval");
		}

		//check dimensions
		size_t rowsize = width * 3;
		size_t size = rowsize * height; //TODO: overflow?
		bool use_short = maxval > 0xFF;
		if (use_short) size *= 2;
		if (maxval > 0xFFFF) {
			throw std::runtime_error("PPM: Invalid maxval");
		}
		if (size > maxsize) {
			throw std::runtime_error("PPM: Size overflow");
		}
		
		// 9. Raster (width x height x 3) bytes, x2 if maxval > 255, MSB first
		prep.start(height, width, 3);
		if (use_short) {
			std::vector<uint16_t> row(rowsize, 0);
			do {
				size_t i;
				for (i = 0; i < rowsize; ++i) {
					if (fread(buffer, 1, 2, file) < 2) break;
					row[i] = (buffer[0] << 8) | (buffer[1]); //deal with endianness
				}
				if (i < rowsize) {
					throw std::runtime_error("PPM: Not enough data");
				}
			} while (prep.add_row(row.data()));
		}
		else {
			std::vector<uint8_t> row(rowsize, 0);
			do {
				if (fread(row.data(), 1, rowsize, file) < rowsize) {
					throw std::runtime_error("PPM: Not enough data");
				}
			} while (prep.add_row(row.data()));
		}
		return prep.stop();
	}

	Hash::Hash() : bytes(), bi(8) {}

	void Hash::clear() {
		bytes.clear();
		bi = 8;
	}

	void Hash::append_bit(bool b) {
		if (bi > 7) {
			bytes.push_back(0);
			bi = 0;
		}
		if (b) {
			bytes.back() |= (uint8_t(1) << bi);
		}
		++bi;
	}

	std::vector<uint8_t> BlockHash::apply(const Image<float>& image)
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

		clear();
		bytes.reserve(8);
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
				append_bit(rank >= 4);
			}
			i0 = i1;
			i1 = i2;
			i2 += tmp.row_size;
		}
		return bytes;
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

	std::vector<uint8_t> DCTHash::apply(const Image<float>& image)
	{
		if (image.width != image.height || image.channels != 1) {
			throw std::runtime_error("DCT: image must be square and single-channel");
		}
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

		/* Phase 2: Apply DCT along columns */
		Image<float> dct(M_, M_);
		//iterate over vertical spatial frequencies
		for (size_t v = 0, i = 0; v < M_; ++v, i += dct.row_size) {
			//iterate over horizontal spatial frequencies
			for (size_t u = 0, j = i; u < M_; ++u, ++j) {
				//reduce over image rows
				float dct_uv = 0.0f;
				for (size_t y = 0, k = v, di = u; y < N_; ++y, k += M_, di += M_) {
					dct_uv += m_[k] * dct_1[di];
				}
				dct[j] = dct_uv;
			}
		}

		/* Phase 3: Compute hash */
		clear();
		bytes.reserve((size_t(M_) * M_ + 7) / 8);
		//iterate over the DCT so that we always output the bits in the same order, no matter the size
		// we will start in the corner, and then build up in square shells:
		// 0 1 4
		// 2 3 5
		// 6 7 8
		
		//iterate across the first row
		for (size_t u = 0; u < M_; ++u) {
			//iterate down the column at u, to the (u-1) row
			size_t i = 0;
			for (size_t v = 0; v < u; ++v, i += dct.row_size) {
				append_bit(dct[i + u] > 0);
			}
			//iterate across row v, to column u
			for (size_t uu = 0, j = i; uu < u + 1; ++uu, ++j) {
				append_bit(dct[j] > 0);
			}
		}

		return bytes;
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

void print_hash(std::ostream& out, const std::vector<uint8_t>& hash, const std::string& fname, bool binary, bool quiet) {
	if (binary) {
		for (auto b : hash) out.put(static_cast<char>(b));
	}
	else {
		out << std::hex << std::setfill('0');
		for (auto b : hash) out << std::setw(2) << int(b);
		if (!quiet) out << " " << fname;
		out << "\n";
	}
}

int main(int argc, const char* argv[])
{
	std::vector<std::string> files;
	int dct_size = 1;
	bool even = false;
	bool debug = false;
	bool use_dct = false;
	bool binary = false;
	bool quiet = false;
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
			else if (arg.substr(0, 2) == "-d") {
				use_dct = even = true;
				if (arg.size() > 2) {
					auto size_str = arg.substr(2);
					if (size_str.size() > 1 || !isdigit(size_str[0])) {
						std::cerr << "Invalid dct size. Must be 1,2,3 or 4\n";
						print_usage();
						return 1;
					}
					dct_size = static_cast<size_t>(atoi(size_str.c_str()));
				}
			}
			else if (arg == "--dct") {
				use_dct = even = true;
				if (++i < argc) {
					auto size_str = std::string(argv[i]);
					if (size_str.size() > 1 || !isdigit(size_str[0])) {
						std::cerr << "Invalid dct size. Must be 1,2,3 or 4\n";
						print_usage();
						return 1;
					}
					dct_size = static_cast<size_t>(atoi(size_str.c_str()));
				}
				else {
					std::cerr << "Missing dct size";
					print_usage();
					return 1;
				}
			}
			else if (arg == "-q" || arg == "--quiet") quiet = true;
			else if (arg == "-x") binary = true;
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

	if (use_dct && (dct_size < 1 || dct_size > 4)) {
		std::cerr << "Invalid dct size. Must be 1,2,3 or 4\n";
		print_usage();
		return 1;
	}

	Preprocess prep(128, 128);

	std::unique_ptr<Hash> hash;
	if (use_dct) hash = std::make_unique<DCTHash>(8 * dct_size, even);
	else hash = std::make_unique<BlockHash>();

	int ret = 0;
	if (files.empty()) {
	#ifdef _WIN32
		auto result = _setmode(_fileno(stdin), _O_BINARY);
		if (result < 0) {
			std::cerr << "Failed to open stdin in binary mode";
			return 1;
		}
	#else
		if (!freopen(nullptr, "rb", stdin)) {
			std::cerr << "Failed to open stdin in binary mode";
			return 1;
		}
	#endif
		try {
			Image<float> img;
			std::string file; 
			img = load_ppm(stdin, prep);
			while (img.size > 0) {
				print_hash(std::cout, hash->apply(img), file, binary, true);
				img = load_ppm(stdin, prep, false); //it's OK to get an empty file here
			}
		}
		catch (std::exception& e) {
			std::cerr << "Exception while processing input:\n";
			std::cerr << e.what();
			ret = -1;
		}
		catch (...) {
			std::cerr << "Unknown exception while processing input:\n";
			ret = -1;
		}
	}
	else {
		for (const auto& file : files) {
			try {
				Image<float> img = load(file, prep);
				if (debug) save(file + ".pgm", img, 1.0f);
				print_hash(std::cout, hash->apply(img), file, binary, quiet);
			}
			catch (std::exception& e) {
				std::cerr << "Exception while processing file: " << file << "\n";
				std::cerr << e.what();
				ret = -1;
			}
			catch (...) {
				std::cerr << "Unknown exception while processing file: " << file << "\n";
				ret = -1;
			}
		}
	}
	return ret;
}