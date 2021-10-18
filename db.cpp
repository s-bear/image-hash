
#include "db.h"
#include "SQLiteCpp/SQLiteCpp.h"

#include <algorithm>

namespace imghash {
	

	//Open the database
	Database::Database(const std::string& path)
		: impl(std::make_unique<Impl>(path))
	{
		//nothing else to do
	}

	//Close the database
	Database::~Database()
	{

	}

	//Add a file
	void Database::insert(const point_type& point, const item_type& item)
	{
		impl->insert(point, item);
	}

	//Find similar images
	std::vector<Database::item_type> Database::query(const point_type& point, unsigned int dist, size_t limit)
	{
		return impl->query(point, dist, static_cast<int64_t>(limit));
	}

	//Add a vantage point for querying
	void Database::add_vantage_point(const point_type& point)
	{
		impl->add_vantage_point(point);
	}

	//Find a point that would make a good vantage point
	Database::point_type Database::find_vantage_point(size_t sample_size)
	{
		return impl->find_vantage_point(sample_size);
	}
	
}