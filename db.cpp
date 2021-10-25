
#include "db.h"
#include "SQLiteCpp/SQLiteCpp.h"
#include "mvptable.h"

#include <algorithm>



namespace imghash {

	class Database::Impl {
		std::shared_ptr<SQLite::Database> db;
		MVPTable table;
	public:
		Impl(const std::string& path);
		void insert(const point_type& point, const item_type& item);
		query_result query(const point_type& point, unsigned int dist, size_t limit = 10);
	};

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
	Database::query_result Database::query(const point_type& point, unsigned int dist, size_t limit)
	{
		return impl->query(point, dist, static_cast<int64_t>(limit));
	}

	Database::Impl::Impl(const std::string& path)
		: db(std::make_shared<SQLite::Database>(path, SQLite::OPEN_CREATE)),
		table()
	{
		SQLite::Transaction trans(*db);
		table = MVPTable(db, Hash::distance);

		db->exec(
			"CREATE TABLE IF NOT EXISTS images ("
				"path TEXT PRIMARY KEY,"
				"mvp_id INTEGER,"
				"FOREIGN KEY(mvp_id) REFERENCES mvp_points(id)"
			") WITHOUT ROWID;"
		);

		trans.commit();
	}

	void Database::Impl::insert(const point_type& point, const item_type& item)
	{

	}

	Database::query_result Database::Impl::query(const point_type& point, unsigned int dist, size_t limit)
	{
		return {};
	}
}