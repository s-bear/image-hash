
#include "db.h"
#include "SQLiteCpp/SQLiteCpp.h"

namespace imghash {
	class Database::Impl {
	public:
		SQLite::Database db;
		
		//used in insert()
		std::unique_ptr<SQLite::Statement> insert_point, get_point_id_by_value, add_points_col, insert_file, update_file, get_file_by_path;
		
		//used in add_vp()
		std::unique_ptr<SQLite::Statement> get_all_point_values, insert_vantage_point, get_vantage_points, update_point;
		
		
		std::unique_ptr<SQLite::Statement> get_file_by_pointid;
		std::unique_ptr<SQLite::Statement> get_count, increment_count;

		std::vector<int64_t> vp_ids_;

		std::unique_ptr<SQLite::Statement> make_stmt(const std::string& stmt)
		{
			return std::make_unique<SQLite::Statement>(db, stmt);
		}

		void make_insert_point(const std::vector<int64_t>& vp_ids)
		{
			if (std::equal(vp_ids_.begin(), vp_ids_.end(), vp_ids.begin(), vp_ids.end())) {
				return;
			}
			else {
				// INSERT INTO points(value, d0, d1, ...) VALUES (?, ?, ?, ...) RETURNING id;
				std::string stmt1 = "INSERT INTO points(value";
				std::string stmt2 = ") VALUES (?";
				for (int64_t id : vp_ids) {
					auto id_str = std::to_string(id);
					stmt1 += ", d" + id_str;
					stmt2 += ", ?";
				}
				stmt2 += ") RETURNING id;";
				insert_point = make_stmt(stmt1 + stmt2);
				vp_ids_ = vp_ids;
			}
		}

		Impl(const std::string& path)
			: db(path, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE)
		{
			//initialize database as necessary
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

				"CREATE TABLE IF NOT EXISTS files ("
					"path TEXT PRIMARY KEY,"
					"pointid INTEGER,"
					"FOREIGN KEY(pointid) REFERENCES points(id)"
				") WITHOUT ROWID;"

				"CREATE INDEX IF NOT EXISTS"
					"idx_files_point ON files(pointid);"
				");"
			);
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

			std::vector<int64_t> vp_ids;

			SQLite::Statement get_vp_ids(db, "SELECT id FROM vantage_points ORDER BY id ASC;");
			while (get_vp_ids.executeStep()) {
				vp_ids.push_back(get_vp_ids.getColumn(0).getInt64());
			}

			trans.commit();

			//precompile statements
			make_insert_point(vp_ids);
			get_all_point_values = make_stmt("SELECT id, value FROM points;");
			get_point_id_by_value = make_stmt("SELECT id FROM points WHERE value = $value;");
			update_point = make_stmt("UPDATE points SET $col = $val WHERE id = $id;");

			insert_vantage_point = make_stmt("INSERT INTO vantage_points(value) VALUES($value) RETURNING id;");
			get_vantage_points = make_stmt("SELECT id, value FROM vantage_points ORDER BY id ASC;");
			add_points_col = make_stmt("ALTER TABLE points ADD COLUMN $col INTEGER DEFAULT 0xFFFFFFFF; CREATE INDEX $idx ON points($col);");

			insert_file = make_stmt("INSERT INTO files(path, pointid) VALUES($path, $pointid);");
			update_file = make_stmt("UPDATE files SET pointid = $pointid WHERE path = $path;");
			get_file_by_path = make_stmt("SELECT pointid FROM files where path = $path;");
			get_file_by_pointid = make_stmt("SELECT path FROM files WHERE pointid = $pointid;");
			
			get_count = make_stmt("SELECT points, vantage_points, files FROM counts WHERE id = 1;");
			increment_count = make_stmt("UPDATE counts SET $col = $col + 1 WHERE id = 1;");
		}

		point_type get_point_value(SQLite::Column& col) {
			size_t n = col.getBytes();
			const uint8_t* data = static_cast<const uint8_t*>(col.getBlob());
			return point_type(data, data + n);
		}

		void bind_point_value(SQLite::Statement& stmt, const char* n, const point_type& p)
		{
			stmt.bind(n, p.data(), p.size());
		}

		void bind_point_value(SQLite::Statement& stmt, const int n, const point_type& p)
		{
			stmt.bind(n, p.data(), p.size());
		}

		uint32_t get_distance(const point_type& p1, const point_type& p2)
		{
			return Hash::distance(p1, p2);
		}

		void insert(const point_type& p_value, const std::string& path)
		{
			SQLite::Transaction trans(db);

			//is the hash already in the database?
			bind_point_value(*get_point_id_by_value, "value", p_value);
			int64_t point_id = -1;
			get_point_id_by_value->reset();
			if (get_point_id_by_value->executeStep()) {
				//yes
				point_id = get_point_id_by_value->getColumn(0).getInt64();
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
					//copy the blob into a hash_type
					auto vp_value = get_point_value(get_vantage_points->getColumn(1)); //TODO: use span to avoid copies
					uint32_t d = get_distance(vp_value, p_value);
					dists.push_back(d);
				}
				
				make_insert_point(vp_ids); //update the insert_point statement if vp_ids changed
				bind_point_value(*insert_point, 1, p_value);
				for (size_t i = 0; i < dists.size(); ++i) {
					insert_point->bind(i + 2, dists[i]); //the first parameter has index 1, so these start at 2
				}
				insert_point->reset();
				if (insert_point->executeStep()) {
					point_id = insert_point->getColumn(0).getInt64();
					
					increment_count->bind("col", "points");
					increment_count->reset();
					increment_count->exec();
				}
				else {
					throw std::runtime_error("Error inserting point");
				}
				//done adding the hash
			}

			//is the file already in the database?
			get_file_by_path->bind("path", path);
			get_file_by_path->reset();
			if (get_file_by_path->executeStep()) {
				//yes, does the pointid match?
				auto old_id = get_file_by_path->getColumn(0).getInt64();
				if (old_id != point_id) {
					//no -- update it
					update_file->bind("path", path);
					update_file->bind("pointid", point_id);
					update_file->reset();
					update_file->exec();

				}
			}
			else {
				//no: we need to insert the file
				insert_file->bind("path", path);
				insert_file->bind("pointid", point_id);
				insert_file->reset();
				insert_file->exec();

				increment_count->bind("col", "files");
				increment_count->reset();
				increment_count->exec();
			}
			
			//done
			trans.commit();
		}

		void add_vp(const point_type& vp_value)
		{
			SQLite::Transaction trans(db);
			
			bind_point_value(*insert_vantage_point, "value", vp_value);
			int64_t vp_id = -1;
			insert_vantage_point->reset();
			if (insert_vantage_point->executeStep()) {
				vp_id = insert_vantage_point->getColumn(0).getInt64();

				increment_count->bind("col", "vantage_points");
				increment_count->reset();
				increment_count->exec();
			}
			else {
				throw std::runtime_error("Error inserting vantage point");
			}
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

			trans.commit();
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
	void Database::insert(const Hash::hash_type& hash, const std::string& name)
	{
		impl->insert(hash, name);
	}

	//Find similar images
	void Database::query(const Hash::hash_type& hash, unsigned int dist)
	{

	}

	//Add a vantage point for querying
	void Database::add_vantage_point(const Hash::hash_type& hash)
	{
		impl->add_vp(hash);
	}

	//Find a point that would make a good vantage point
	Hash::hash_type Database::find_vantage_point(size_t sample_size)
	{
		return impl->find_vantage_point(sample_size);
	}
	
}