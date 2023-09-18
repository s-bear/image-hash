
#pragma once
#include "imghash.h"

#include <string>
#include <utility>
#include <memory>

namespace imghash {

	class HashDatabase {
		class Impl;
		std::unique_ptr<Impl> impl;
	public:
		using point_type = Hasher::hash_type;
		using item_type = std::string;
		using query_result = std::tuple<int32_t, item_type, int32_t>;

		//Open the database
		HashDatabase(const std::string& path);

		//Close the database
		~HashDatabase();

		//check that the database was created with the given hash type
		// if no hash type is in the database, (ie. first use) then sets the hash type
		bool check_hash_type(const std::string& hash_type_str);

		//Add a file
		void insert(const point_type& point, const item_type& item);

		void rename(const item_type& item1, const item_type& item2);
		void remove(const item_type& item);
		bool exists(const item_type& item);

		//Find similar items
		std::vector<query_result> query(const point_type& point, unsigned int dist, size_t limit = 10);
	};
}
