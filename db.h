
#pragma once
#include "imghash.h"

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

namespace imghash {

	class Database {
		class Impl;
		std::unique_ptr<Impl> impl;
	public:
		//Open the database
		Database(const std::string& path);

		//Close the database
		~Database();

		//Add a file
		void insert(const Hash::hash_type& hash, const std::string& name);

		//Find similar images
		void query(const Hash::hash_type& hash, unsigned int dist);

		//Add a vantage point for querying
		void add_vantage_point(const Hash::hash_type& hash);

		//Find a point that would make a good vantage point
		Hash::hash_type find_vantage_point();
	};
}
