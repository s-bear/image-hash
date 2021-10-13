
#include "imghash.h"

#include <iostream>
#include <iomanip>

#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

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

	imghash::Preprocess prep(128, 128);

	std::unique_ptr<imghash::Hash> hash;
	if (use_dct) hash = std::make_unique<imghash::DCTHash>(8 * dct_size, even);
	else hash = std::make_unique<imghash::BlockHash>();

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
			imghash::Image<float> img;
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
				imghash::Image<float> img = load(file, prep);
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
