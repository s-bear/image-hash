
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
		void rename(const item_type& item1, const item_type& item2);
		void remove(const item_type& item);
		bool exists(const item_type& item);
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

	void Database::rename(const item_type& item1, const item_type& item2)
	{
		impl->rename(item1, item2);
	}
	void Database::remove(const item_type& item)
	{
		impl->remove(item);
	}
	bool Database::exists(const item_type& item)
	{
		return impl->exists(item);
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
				"id INTEGER PRIMARY KEY,"
				"path TEXT,"
				"count INTEGER"
			");"
			"CREATE UNIQUE INDEX IF NOT EXISTS idx_images_path ON images(path);"
			"CREATE TABLE IF NOT EXISTS map_images_points ("
				"image_id INTEGER,"
				"point_id INTEGER,"
				"image_n INTEGER,"
				"FOREIGN KEY (image_id) REFERENCES images(id),"
				"FOREIGN KEY (point_id) REFERENCES mvp_points(id)"
			");"
		);
	}

	void Database::Impl::rename(const item_type& item1, const item_type& item2) 
	{
		auto& stmt = cache["UPDATE images SET path = $new_path WHERE path = $old_path;"];
		stmt.bind("$new_path", item2);
		stmt.bind("$old_path", item1);
		cache.exec(stmt);
	}

	void Database::Impl::remove(const item_type& item)
	{
		auto& sel_id = cache["SELECT id FROM images WHERE path = $path;"];
		sel_id.bind("$path", item);
		auto id = cache.exec_getInt64(sel_id, "id");
		auto& del = cache[
			"DELETE FROM map_images_points WHERE image_id = $id;"
			"DELETE FROM images WHERE id = $id;"
		];
		del.bind("$id", id);
		cache.exec(del);
	}
	bool Database::Impl::exists(const item_type& item)
	{
		auto& sel_id = cache["SELECT id FROM images WHERE path = $path;"];
		sel_id.bind("$path", item);
		if (sel_id.executeStep()) {
			sel_id.reset();
			return true;
		}
		else {
			sel_id.reset();
			return false;
		}
	}
	void Database::Impl::insert(const point_type& point, const item_type& path)
	{
		// the first point inserted will also be the first vantage point
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

		//is the path already in images?
		auto& sel_image = cache["SELECT id, count FROM images WHERE path = $path;"];
		int64_t image_id = -1;
		int32_t image_count = -1; //the count for this image before insertion
		sel_image.bind("$path", path);
		if (sel_image.executeStep()) {
			//yes
			image_id = sel_image.getColumn("id").getInt64();
			image_count = sel_image.getColumn("count").getInt();
			sel_image.reset();
			//we're adding a new point to the image, so increment its count
			auto& inc_count = cache["UPDATE images SET count = count + 1 WHERE id = $id;"];
			inc_count.bind("$id", image_id);
			cache.exec(inc_count);
		}
		else {
			sel_image.reset();
			//no -- insert a new entry into images
			auto& ins_image = cache["INSERT INTO images(path, count) VALUES($path, 1) RETURNING id;"];
			ins_image.bind("$path", path);
			image_id = cache.exec_getInt64(ins_image, "id");
			image_count = 0;
		}
		auto& ins_map = cache[
			"INSERT INTO map_images_points(image_id, point_id, image_n)"
				"VALUES($img_id, $pt_id, $img_n);"
		];
		ins_map.bind("$img_id", image_id);
		ins_map.bind("$pt_id", point_id);
		ins_map.bind("$img_n", image_count);
		cache.exec(ins_map);
	}

	std::vector<Database::query_result> Database::Impl::query(const point_type& point, unsigned int dist, size_t limit)
	{
		//table.query populates temp.mvp_query(id, dist)
		// where the ids refer to point_id in map_images_points
		// If an image has multiple point entries (as in a video) we want only the best matching one
		table.query(point, dist);
		auto& sel_query = cache[
			"WITH image_query AS ("
				"SELECT m.image_id AS id, m.image_n AS n, MIN(q.dist) AS dist "
				"FROM map_images_points m, temp.mvp_query q ON q.id = m.point_id "
				"GROUP BY m.image_id"
			") SELECT i.path AS path, iq.n AS n, iq.dist AS dist "
			"FROM images i, image_query iq ON i.id = iq.id "
			"ORDER BY dist LIMIT $limit;"
		];

		std::vector<query_result> result;
		
		sel_query.bind("$limit", static_cast<int64_t>(limit));
		while (sel_query.executeStep()) {
			std::string path = sel_query.getColumn("path").getString();
			int32_t dist = sel_query.getColumn("dist").getInt();
			int32_t n = sel_query.getColumn("n").getInt();
			result.emplace_back(dist, std::move(path), n);
		}
		sel_query.reset();
		return result;
	}
}