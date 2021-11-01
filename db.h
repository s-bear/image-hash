
#pragma once
#include "imghash.h"

#include <string>
#include <utility>
#include <memory>

namespace imghash {

	class Database {
		class Impl;
		std::unique_ptr<Impl> impl;
	public:
		
		using point_type = Hasher::hash_type;
		using item_type = std::string;
		using query_result = std::pair<int32_t, item_type>;

		//Open the database
		Database(const std::string& path);

		//Close the database
		~Database();

		//Add a file
		void insert(const point_type& point, const item_type& data);

		//Find similar items
		std::vector<query_result> query(const point_type& point, unsigned int dist, size_t limit = 10);
	};
}
