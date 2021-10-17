
#include "db.h"
#include "SQLiteCpp/SQLiteCpp.h"

namespace imghash {
	class Database::Impl {
	protected:

		/* SQL BLOB <-> point_type interface*/
		
		// Convert the value BLOB into point_type
		static point_type get_point_value(SQLite::Column& col) {
			size_t n = col.getBytes();
			const uint8_t* data = static_cast<const uint8_t*>(col.getBlob());
			return point_type(data, data + n);
		}

		// Convert point_type into a BLOB
		static const void* get_point_data(const point_type& p) {
			return p.data();
		}
		static int get_point_size(const point_type& p) {
			return static_cast<int>(p.size());
		}

		// Get the distance between two points
		static uint32_t get_distance(const point_type& p1, const point_type& p2)
		{
			return Hash::distance(p1, p2);
		}

		static item_type get_item(SQLite::Column& col) {
			return col.getString();
		}

		//The database connection
		SQLite::Database db;
		
		using stmt_ptr = std::unique_ptr<SQLite::Statement>;

		//Cached statements
		stmt_ptr insert_point, get_point_id_by_value, add_points_col, insert_file, update_file, get_file_by_path;
		stmt_ptr get_all_point_values, insert_vantage_point, get_vantage_points, update_point;
		stmt_ptr partition_points, clear_query, insert_query, get_files_by_query;

		// Cached vantage-point IDs, for insert_point 
		std::vector<int64_t> vp_ids_;

		stmt_ptr get_count, increment_count;

		stmt_ptr make_stmt(const std::string& stmt)
		{
			return std::make_unique<SQLite::Statement>(db, stmt);
		}

		//update cached vp_ids
		// deletes insert_point and parition_points if it changes
		void update_vp_ids(const std::vector<int64_t>& vp_ids)
		{
			if (!std::equal(vp_ids_.begin(), vp_ids_.end(), vp_ids.begin(), vp_ids.end())) {
				vp_ids_ = vp_ids;
				insert_point.reset();
				partition_points.reset();
			}
		}

		// INSERT INTO points(value, d0, d1, ...) VALUES (?, ?, ?, ...) RETURNING id;
		// Where d0, d1, ... are "d{id}" for id in vp_ids
		stmt_ptr make_insert_point(const std::vector<int64_t>& vp_ids)
		{
			std::string stmt1 = "INSERT INTO points(value";
			std::string stmt2 = ") VALUES (?";
			for (int64_t id : vp_ids) {
				auto id_str = std::to_string(id);
				stmt1 += ", d" + id_str;
				stmt2 += ", ?";
			}
			stmt2 += ") RETURNING id;";
			return make_stmt(stmt1 + stmt2);
		}

		// SELECT id, value FROM points WHERE (d0 BETWEEN ? AND ?) AND (d1 BETWEEN ? AND ?) AND ...;
		// Where d0, d1, ... are "d{id}" for id in vp_ids
		stmt_ptr make_partition_points(const std::vector<int64_t>& vp_ids)
		{
			std::string stmt = "SELECT id, value FROM points";
			std::string pfx = " WHERE ";
			for (int64_t id : vp_ids) {
				stmt += pfx + "(d" + std::to_string(id) + " BETWEEN ? AND ?)";
				pfx = " AND ";
			}
			stmt += ";";
			return make_stmt(stmt);
		}

		void init_tables()
		{
			SQLite::Transaction trans(db);
			db.exec(
				"CREATE TABLE IF NOT EXISTS counts ("
					"id INTEGER PRIMARY KEY,"
					"points INTEGER,"
					"vantange_points INTEGER,"
					"files INTEGER"
				");"

				"CREATE TABLE IF NOT EXISTS points ("
					"id INTEGER PRIMARY KEY,"
					"value BLOB"
				");"

				"CREATE UNIQUE INDEX IF NOT EXISTS idx_points_value ON points(value);"

				"CREATE TABLE IF NOT EXISTS vantage_points ("
					"id INTEGER PRIMARY KEY,"
					"value BLOB UNIQUE"
				");"

				"CREATE TABLE temp.query ("
					"id INTEGER PRIMARY KEY,"
					"dist INTEGER"
				");"
				"CREATE INDEX temp.idx_query_dist ON temp.query(dist);"

				"CREATE TABLE IF NOT EXISTS files ("
					"path TEXT PRIMARY KEY,"
					"pointid INTEGER,"
					"FOREIGN KEY(pointid) REFERENCES points(id)"
				") WITHOUT ROWID;"

				"CREATE INDEX IF NOT EXISTS"
					"idx_files_point ON files(pointid);"
				");"
			);
			//if counts is empty, initialize it with a 
			if (db.execAndGet("SELECT COUNT(1) FROM counts;").getInt64() == 0)
			{
				auto num_points = db.execAndGet("SELECT COUNT(1) FROM points;").getInt64();
				auto num_vantage_points = db.execAndGet("SELECT COUNT(1) FROM vantage_points;").getInt64();
				auto num_files = db.execAndGet("SELECT COUNT(2) FROM files;").getInt64();
				auto insert_counts = make_stmt("INSERT INTO counts(points,vantage_points,files) VALUES(?, ?, ?);");
				insert_counts->bind(1, num_points);
				insert_counts->bind(2, num_vantage_points);
				insert_counts->bind(3, num_files);
				insert_counts->exec();
			}
			//get all of the vantage point ids
			std::vector<int64_t> vp_ids;
			SQLite::Statement get_vp_ids(db, "SELECT id FROM vantage_points ORDER BY id ASC;");
			while (get_vp_ids.executeStep()) {
				vp_ids.push_back(get_vp_ids.getColumn(0).getInt64());
			}
			vp_ids_ = std::move(vp_ids);
			trans.commit();
		}

		// Insert a point into points, if it doesn't already exist, returning the id
		// No transaction
		int64_t insert_point_(const point_type& p_value)
		{
			//is the point already in the database?
			get_point_id_by_value->bind("value", get_point_data(p_value), get_point_size(p_value));

			get_point_id_by_value->reset();
			if (get_point_id_by_value->executeStep()) {
				//yes
				return get_point_id_by_value->getColumn(0).getInt64();
			}
			else {
				//no: we need to add the point

				//Calculate distances to each vantage point
				std::vector<int64_t> vp_ids;
				std::vector<uint32_t> dists;

				get_vantage_points->reset();
				while (get_vantage_points->executeStep()) {
					auto id = get_vantage_points->getColumn(0).getInt64(); //vantage point id
					vp_ids.push_back(id);

					auto vp_value = get_point_value(get_vantage_points->getColumn(1)); //vantage point value
					uint32_t d = get_distance(vp_value, p_value);
					dists.push_back(d);
				}
				update_vp_ids(vp_ids); //check to see if they changed since the last call

				//update the insert_point statement if vp_ids changed
				if (!insert_point) insert_point = make_insert_point(vp_ids);

				insert_point->bind(1, get_point_data(p_value), get_point_size(p_value));
				for (size_t i = 0; i < dists.size(); ++i) {
					insert_point->bind(i + 2, dists[i]); //the first parameter has index 1, so these start at 2
				}
				insert_point->reset();
				if (insert_point->executeStep()) {
					//increment the point count
					increment_count->bind("col", "points");
					increment_count->reset();
					increment_count->exec();

					return insert_point->getColumn(0).getInt64();
				}
				else {
					throw std::runtime_error("Error inserting point");
				}
			}
		}

		// Insert an item, updates the point_id if the item already exists
		// No transaction
		void insert_item_(int64_t point_id, const item_type& item)
		{
			//is the item already in the database?
			get_file_by_path->bind("path", item);
			get_file_by_path->reset();
			if (get_file_by_path->executeStep()) {
				//yes, does the pointid match?
				auto old_id = get_file_by_path->getColumn(0).getInt64();
				if (old_id != point_id) {
					//no -- update it
					update_file->bind("path", item);
					update_file->bind("pointid", point_id);
					update_file->reset();
					update_file->exec();
				}
			}
			else {
				//no: we need to insert the file
				insert_file->bind("path", item);
				insert_file->bind("pointid", point_id);
				insert_file->reset();
				insert_file->exec();

				increment_count->bind("col", "files");
				increment_count->reset();
				increment_count->exec();
			}
		}

		// Insert a vantage point into points, throws an exception if it already exists
		// No transaction
		int64_t insert_vantage_point_(const point_type& vp_value)
		{
			insert_vantage_point->bind("value", get_point_data(vp_value), get_point_size(vp_value));
			insert_vantage_point->reset();
			if (insert_vantage_point->executeStep()) {
				increment_count->bind("col", "vantage_points");
				increment_count->reset();
				increment_count->exec();

				return insert_vantage_point->getColumn(0).getInt64();
			}
			else {
				throw std::runtime_error("Error inserting new vantage_point");
			}
		}

		// Add a new column to points for the given vantage_point
		//  The new column is named "d{vp_id}" and will be populated by get_distance(vp_value, points.value)
		// No transaction
		void add_points_column_(int64_t vp_id, const point_type& vp_value)
		{
			std::string col_name = "d" + std::to_string(vp_id);

			add_points_col->bind("col", col_name);
			add_points_col->bind("idx", "idx_" + col_name);
			add_points_col->reset();
			add_points_col->exec();

			// we need to compute the distance from the new vantage point to all of the existing points
			update_point->bind("col", col_name);

			get_all_point_values->reset();
			while (get_all_point_values->executeStep()) {
				auto id = get_all_point_values->getColumn(0).getInt64();
				auto p_value = get_point_value(get_all_point_values->getColumn(1));

				uint32_t d = get_distance(vp_value, p_value);
				update_point->bind("id", id);
				update_point->bind("val", d);
				update_point->reset();
				update_point->exec();
			}
		}

	public:
		// Construct, open or create the database
		Impl(const std::string& path)
			: db(path, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE)
		{
			//initialize database as necessary
			init_tables();

			//precompile statements
			insert_point = make_insert_point(vp_ids_);
			get_all_point_values = make_stmt("SELECT id, value FROM points;");
			get_point_id_by_value = make_stmt("SELECT id FROM points WHERE value = $value;");
			update_point = make_stmt("UPDATE points SET $col = $val WHERE id = $id;");

			insert_vantage_point = make_stmt("INSERT INTO vantage_points(value) VALUES($value) RETURNING id;");
			get_vantage_points = make_stmt("SELECT id, value FROM vantage_points ORDER BY id ASC;");
			add_points_col = make_stmt("ALTER TABLE points ADD COLUMN $col INTEGER DEFAULT 0xFFFFFFFF; CREATE INDEX $idx ON points($col);");

			partition_points = make_partition_points(vp_ids_);
			clear_query = make_stmt("DELETE FROM temp.query;");
			insert_query = make_stmt("INSERT INTO temp.query(id, dist) VALUES($id, $dist)");
			get_files_by_query = make_stmt("SELECT path FROM files WHERE pointid = (SELECT id FROM temp.query ORDER BY dist LIMIT $limit);");

			insert_file = make_stmt("INSERT INTO files(path, pointid) VALUES($path, $pointid);");
			update_file = make_stmt("UPDATE files SET pointid = $pointid WHERE path = $path;");
			get_file_by_path = make_stmt("SELECT pointid FROM files where path = $path;");
			
			get_count = make_stmt("SELECT points, vantage_points, files FROM counts WHERE id = 1;");
			increment_count = make_stmt("UPDATE counts SET $col = $col + 1 WHERE id = 1;");
		}

		
		// Insert a (point, item) pair
		//  Multiple items may be associated with the same point
		//  If the point is new, its distance to all existing vantage points will be computed and stored
		void insert(const point_type& p_value, const item_type& item)
		{
			SQLite::Transaction trans(db);
			auto point_id = insert_point_(p_value);
			insert_item_(point_id, item);
			trans.commit();
		}

		void add_vantage_point(const point_type& vp_value)
		{
			SQLite::Transaction trans(db);
			
			auto vp_id = insert_vantage_point_(vp_value);
			add_points_column_(vp_id, vp_value);

			trans.commit();
		}
	
		std::vector<item_type> query(const point_type& pt, unsigned int radius, int64_t limit)
		{
			//we need the distance from each vantage point to pt
			SQLite::Transaction trans(db);

			std::vector<int64_t> vp_ids;
			std::vector<uint32_t> lower_bounds, upper_bounds;
			get_vantage_points->reset();
			while (get_vantage_points->executeStep()) {
				auto id = get_vantage_points->getColumn(0).getInt64(); //vantage point id
				vp_ids.push_back(id);

				auto vp_value = get_point_value(get_vantage_points->getColumn(1)); //vantage point value
				//get the distance
				uint32_t d = get_distance(vp_value, pt);
				lower_bounds.push_back(d <= radius ? 0 : d - radius);
				upper_bounds.push_back(d >= 0xFFFFFFFF - radius ? 0xFFFFFFFF : d + radius);
			}
			update_vp_ids(vp_ids);
			//we get all of the point within the lower and upper bounds using parition_points
			// for each, we compute the distance from the query point to it, and store
			// the result in the temp.query table
			clear_query->reset();
			clear_query->exec();
			
			if (!partition_points) partition_points = make_partition_points(vp_ids);
			
			for (size_t i = 0; i < vp_ids.size(); ++i) {
				partition_points->bind(2 * i + 1, lower_bounds[i]);
				partition_points->bind(2 * i + 2, upper_bounds[i]);
			}
		
			partition_points->reset();
			while (partition_points->executeStep()) {
				auto id = partition_points->getColumn(0).getInt64();
				auto value = get_point_value(partition_points->getColumn(1));

				uint32_t d = get_distance(pt, value);

				insert_query->bind("id", id);
				insert_query->bind("dist", d);
				insert_query->reset();
				insert_query->exec();
			}

			std::vector<item_type> result;
			get_files_by_query->bind("limit", limit);
			get_files_by_query->reset();
			while (get_files_by_query->executeStep()) {
				result.push_back(get_item(get_files_by_query->getColumn(0)));
			}

			trans.commit();

			return result;
		}

		point_type find_vantage_point(size_t sample_size)
		{
			SQLite::Transaction trans(db);

			// do we have any vantage points yet?
			int64_t num_vantage_points = -1;
			get_count->reset();
			if (get_count->executeStep()) {
				num_vantage_points = get_count->getColumn(1).getInt64();
			}
			else {
				throw std::runtime_error("Error reading number of vantage points");
			}

			if (num_vantage_points > 0) {
				//we need to find a point that's far from all of the existing vantage points

			}
			else {
				//we need to find a point that's far from all other points
			}

			trans.commit();
		}
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