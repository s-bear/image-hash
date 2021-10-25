
#pragma once
#include "imghash.h"

#include <string>
#include <memory>

namespace imghash {

	class Database {
		class Impl;
		std::unique_ptr<Impl> impl;
	public:
		
		using point_type = Hash::hash_type;
		using item_type = std::string;
		using query_result = std::vector<item_type>;

		//Open the database
		Database(const std::string& path);

		//Close the database
		~Database();

		//Add a file
		void insert(const point_type& point, const item_type& data);

		//Find similar items
		query_result query(const point_type& point, unsigned int dist, size_t limit = 10);
	};
}
