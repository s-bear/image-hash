
#include "db.h"
#include "SQLiteCpp/SQLiteCpp.h"
#include "mvptable.h"

#include <algorithm>

namespace imghash {

	class Database::Impl {
		std::shared_ptr<SQLite::Database> db;
		SQLStatementCache cache;

		MVPTable table;
	public:
		Impl(const std::string& path);
		void insert(const point_type& point, const item_type& item);
		std::vector<query_result> query(const point_type& point, unsigned int dist, size_t limit = 10);
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
		//nothing else to do
	}

	//Add a file
	void Database::insert(const point_type& point, const item_type& item)
	{
		impl->insert(point, item);
	}

	//Find similar images
	std::vector<Database::query_result> Database::query(const point_type& point, unsigned int dist, size_t limit)
	{
		return impl->query(point, dist, static_cast<int64_t>(limit));
	}

	Database::Impl::Impl(const std::string& path)
		: db(std::make_shared<SQLite::Database>(path, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE)),
		table(db, Hasher::distance), cache(db)
	{
		db->exec(
			"CREATE TABLE IF NOT EXISTS images ("
				"path TEXT PRIMARY KEY,"
				"mvp_id INTEGER,"
				"FOREIGN KEY(mvp_id) REFERENCES mvp_points(id)"
			") WITHOUT ROWID;"
		);
	}

	void Database::Impl::insert(const point_type& point, const item_type& item)
	{
		// the first point inserted will also be the first vantage point
		//TODO: this is maybe not ideal
		if (table.count_vantage_points() == 0) {
			table.insert_vantage_point(point);
		}
		
		auto point_id = table.insert_point(point);

#ifdef _DEBUG
		int64_t min_balance = 20;
		int64_t vp_target = 5;
#else
		int64_t min_balance = 50;
		int64_t vp_target = 100;
#endif
		table.auto_balance(min_balance, 0.5f);
		table.auto_vantage_point(vp_target);

		auto& ins_image = cache[
			"INSERT OR REPLACE INTO images(path,mvp_id) VALUES($path, $mvp_id);"
		];
		ins_image.bind("$path", item);
		ins_image.bind("$mvp_id", point_id);
		cache.exec(ins_image);
	}

	std::vector<Database::query_result> Database::Impl::query(const point_type& point, unsigned int dist, size_t limit)
	{
		table.query(point, dist);
		auto& sel_query = cache[
			"SELECT i.path AS path, q.dist AS dist FROM temp.mvp_query q, images i ON q.id = i.mvp_id "
			"WHERE dist < $radius ORDER BY dist LIMIT $limit;"
		];
		std::vector<query_result> result;
		
		sel_query.bind("$radius", dist);
		sel_query.bind("$limit", static_cast<int64_t>(limit));
		while (sel_query.executeStep()) {
			std::string path = sel_query.getColumn("path").getString();
			int32_t dist = sel_query.getColumn("dist").getInt();
			result.emplace_back(dist, std::move(path));
		}
		sel_query.reset();
		return result;
	}
}