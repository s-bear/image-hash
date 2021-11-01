
#include "imghash.h"

#include <iostream>
#include <iomanip>
#include <string>
#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

#ifdef USE_SQLITE
#include "db.h"
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
	std::cout << "    -q, --quiet : don't output filename.\n";
	std::cout << "    -n NAME, --name NAME: specify a name for output when reading from stdin\n";
#ifdef USE_SQLITE
	std::cout << "    --db DB_PATH : use the specified database for --add or --query.\n";
	std::cout << "    --add : add the image to the database. If the image comes from stdin, --name must be specified.\n";
	std::cout << "    --query DIST LIMIT: query the database for up to LIMIT similar images within DIST distance.\n";
#endif
	std::cout << "  Supported image formats: \n";
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

#ifdef USE_SQLITE
void print_query(std::ostream& out, const std::vector<imghash::Database::query_result>& results, 
	const std::string& prefix = "  ", const std::string& delim = ": ", const std::string& suffix = "\n")
{
	for (const auto& res : results) {
		out << prefix << res.first << delim << res.second << suffix;
	}
}
#endif

int parse_dct_size(const std::string& s) {
	static const char err_str[] = "Invalid dct size while parsing arguments. Must be 1, 2, 3, or 4.";
	int x;
	try {
		x = std::stoi(s);
	}
	catch (...) {
		throw std::runtime_error(err_str);
	}
	if (x < 1 || x > 4) {
		throw std::runtime_error(err_str);
	}
	return x;
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
	std::string db_path;
	bool add = false;
	unsigned int query_dist = 0;
	size_t query_limit = 0;
	std::string name;
	
	//parse options
	try {
		//TODO: use a proper options parsing library
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
						dct_size = parse_dct_size(arg.substr(2));
					}
				}
				else if (arg == "--dct") {
					use_dct = even = true;
					if (++i < argc) {
						dct_size = parse_dct_size(argv[i]);
					}
					else {
						throw std::runtime_error("Missing dct size. Must be 1,2,3 or 4.");
					}
				}
				else if (arg == "-q" || arg == "--quiet") quiet = true;
				else if (arg == "-n" || arg == "--name") {
					if (++i < argc) {
						name = std::string(argv[i]);
					}
					else {
						throw std::runtime_error("Missing output name.");
					}
				}
				else if (arg == "-x") binary = true;
				else if (arg == "--debug") debug = true;
				else if (arg == "--db") {
					if (++i < argc) {
						db_path = std::string(argv[i]);
					}
					else {
						throw std::runtime_error("Missing database file name.");
					}
				}
				else if (arg == "--add") {
					add = true;
				}
				else if (arg == "--query") {
					if(i + 2 < argc) {
						try {
							query_dist = static_cast<unsigned int>(std::stoul(argv[++i]));
							query_limit = static_cast<size_t>(std::stoull(argv[++i]));
						}
						catch (...) {
							throw std::runtime_error("Invalid query size.");
						}
					}
					else {
						throw std::runtime_error("Missing query distance and/or limit.");
					}
				}
				else {
					throw std::runtime_error("Unknown option: " + arg);
				}
			}
			else {
				files.emplace_back(std::move(arg));
			}
		}

#ifdef USE_SQLITE
		if (db_path.empty() && (add || query_limit > 0)) {
			throw std::runtime_error("--add and --query require --db to be specified.");
		}
#else
		if (!db_path.empty() || add || query_limit > 0) {
			throw std::runtime_error("Support for --db, --add, --query was not compiled. Rebuild with USE_SQLITE defined.");
		}
#endif
	}
	catch (std::exception& e) {
		print_usage();
		std::cerr << "Error while parsing arguments: " << e.what() << std::endl;
		return -1;
	}
	catch (...) {
		print_usage();
		std::cerr << "Unknown error while parsing arguments." << std::endl;
		return -1;
	}
	//done parsing arguments, now do the processing

	try {
		imghash::Preprocess prep(128, 128);

		std::unique_ptr<imghash::Hasher> hasher;
		if (use_dct) hasher = std::make_unique<imghash::DCTHasher>(8 * dct_size, even);
		else hasher = std::make_unique<imghash::BlockHasher>();

#ifdef USE_SQLITE
		std::unique_ptr<imghash::Database> db;
		if (!db_path.empty()) db = std::make_unique<imghash::Database>(db_path);
#endif

		if (files.empty()) {
			//read from stdin
#ifdef _WIN32
			auto result = _setmode(_fileno(stdin), _O_BINARY);
			if (result < 0) {
				throw std::runtime_error("Failed to open stdin in binary mode");
			}
#else
			if (!freopen(nullptr, "rb", stdin)) {
				throw std::runtime_error("Failed to open stdin in binary mode");
			}
#endif
			
			imghash::Image<float> img;
			
			img = load_ppm(stdin, prep);
			while (img.size > 0) {
				auto hash = hasher->apply(img);
				print_hash(std::cout, hash, name, binary, quiet);
				if (db) {
					if (add) db->insert(hash, name);
					if (query_limit > 0) print_query(std::cout, db->query(hash, query_dist, query_limit));
				}
				img = load_ppm(stdin, prep, false); //it's OK to get an empty file here
			}
		}
		else {
			//read from list of files
			for (const auto& file : files) {
				imghash::Image<float> img = load(file, prep);
				
				auto hash = hasher->apply(img);
				print_hash(std::cout, hash, file, binary, quiet);
				if (db) {
					if (add) db->insert(hash, file);
					if (query_limit > 0) print_query(std::cout, db->query(hash, query_dist, query_limit));
				}
			}
		}
	}
	catch (std::exception& e) {
		std::cerr << "Error while processing image: " << e.what() << std::endl;
		return -1;
	}
	catch (...) {
		std::cerr << "Unknown error while processing image." << std::endl;
		return -1;
	}

	return 0;
}
